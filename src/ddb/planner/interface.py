from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from ..util import OptionsBase
from ..validator import valexpr, Lop, QLop, CreateTableLop, ShowTablesLop, AnalyzeStatsLop, CreateIndexLop, InsertLop, DeleteLop, LiteralTableLop, SFWGHLop
from ..executor import Pop, QPop, StatementContext, CreateTablePop, ShowTablesPop, AnalyzeStatsPop, CreateIndexPop, InsertPop, DeletePop, LiteralTablePop, MaterializePop, ProjectPop

class PlannerException(Exception):
    pass

class Planner(ABC):
    """A planner that converts a logical representation of a validated statement (:class:`.Lop`) to an executable form (:class:`.Pop`).
    This base class defines the interface and provides some basic features,
    but the most important function of query planning should be provided by more specialized subclasses.
    """

    @dataclass
    class Options(OptionsBase):
        """A class for options understood by the planner.
        """
        index_join: bool = field(default=True, metadata={'on': True, 'off': False})
        """Whether to enable index-based joins.
        """
        sort_merge_join: bool = field(default=True, metadata={'on': True, 'off': False})
        """Whether to enable sort-merge joins.
        """
        hash_join: bool = field(default=True, metadata={'on': True, 'off': False})
        """Whether to enable hash joins.
        """

    options = Options()
    """Options understood by the planner.
    """

    @classmethod
    def plan(cls, context: StatementContext, lop: Lop) -> Pop:
        """Convert the logical plan specified by ``lop`` into an optimized physical plan for execution.
        """
        pop: Pop
        if isinstance(lop, CreateTableLop):
            return CreateTablePop(context, lop.base_metadata)
        elif isinstance(lop, ShowTablesLop):
            return ShowTablesPop(context)
        elif isinstance(lop, AnalyzeStatsLop):
            return AnalyzeStatsPop(context, lop.base_metas)
        elif isinstance(lop, CreateIndexLop):
            return CreateIndexPop(context, lop.base_metadata, lop.column_index)
        elif isinstance(lop, InsertLop):
            if isinstance(lop.contents, LiteralTableLop):
                return InsertPop(context, lop.base_metadata, LiteralTablePop(context, None, lop.contents.metadata(), lop.contents.rows))
            elif isinstance(lop.contents, SFWGHLop):
                contents_pop = cls.optimize_block(context, lop.contents)
                return InsertPop(
                    context, lop.base_metadata,
                    # use blocking MaterializePop to decouple computation of what to insert and actual insert operations:
                    MaterializePop(
                        # use ProjectPop to ensure precise typing as needed:
                        ProjectPop(
                            contents_pop,
                            [
                                valexpr.cast_if_needed(valexpr.RelativeColumnRef(0, i, stype), ttype)
                                for i, (stype, ttype) in enumerate(zip(
                                    contents_pop.compiled.output_metadata.column_types,
                                    lop.base_metadata.column_types))
                            ],
                            None),
                        blocking=True))
            else:
                raise PlannerException('supported INSERT subquery')
        elif isinstance(lop, DeleteLop):
            # use blocking MaterializePop to decouple computation of what to delete and actual delete operations:
            return DeletePop(context, lop.base_metadata, MaterializePop(cls.optimize_query(context, lop.key_query), blocking=True))
        elif isinstance(lop, SFWGHLop):
            return cls.optimize_block(context, lop)
        else:
            raise PlannerException(f'not yet supported: {type(lop).__name__}')
    
    @classmethod
    def optimize_query(cls, context: StatementContext, query: QLop) -> QPop:
        """Convert the logical query plan specified by ``query`` into an optimized physical plan for execution.
        """
        if isinstance(query, SFWGHLop):
            return cls.optimize_block(context, query)
        else:
            raise PlannerException('query type currently unsupported by planner')

    @classmethod
    @abstractmethod
    def optimize_block(cls, context: StatementContext, block: SFWGHLop) -> QPop:
        """Optimize a single query SELECT-FROM-WHERE-GROUPBY-HAVING block and return the result physical plan.
        """
        pass
