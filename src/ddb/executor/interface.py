"""This module mostly defines *abstract* classes and documents the execution API.
Other modules in the same subpackage define implementation classes.
"""
from typing import final, TypeVar, Generic, Self, Final, Iterable, Generator, Any
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property

from ..globals import ANSI
from ..util import CustomInitMeta
from ..storage import StorageManager
from ..metadata import MetadataManager, TableMetadata
from ..stats import StatsManager, TableStats, CollectionStats
from ..validator import valexpr, ValExpr, OutputLineage
from ..transaction import Transaction

class ExecutorException(Exception):
    """Exceptions thrown at execution time, mostly by functions in the :mod:`.executor` package.
    """
    pass

@dataclass(frozen=True)
class StatementContext:
    """A context for each statement being processed,
    which contains various handles/references to useful objects/information.
    """
    sm: StorageManager
    mm: MetadataManager
    zm: StatsManager[TableStats, CollectionStats]
    tx: Transaction
    tmp_tx: Transaction

class Pop(ABC, metaclass=CustomInitMeta):
    """An object representing a statement --- either a query (:class:`.QPop`) or
    a command (:class:`.CPop`) --- in an executable form.
    """
    @abstractmethod
    def __init__(self, context: StatementContext) -> None:
        self.context: Final = context
        return

    @abstractmethod
    def pstr(self) -> Iterable[str]:
        """Produce a sequence of lines for pretty-printing the object.
        """
        pass

P = TypeVar('P', bound='QPop.CompiledProps', covariant=True)
"""Type variable specifying the type of compiled properties used by :class:`.QPop`.
"""

class QPop(Pop, Generic[P]):
    """A physical query plan operator.  When executed, returns a list of rows.
    """

    @dataclass
    class CompiledProps:
        """Compiled properties of the operator, available by calling :meth:`.QPop.compiled`.
        """
        output_metadata: TableMetadata
        """Metadata describing the output table schema from this operator.
        """
        output_lineage: OutputLineage
        """A mapping from each output column index to its lineage (from original input tables in FROM).
        If a :class:`.NamedColumnRef` is found in an output column's lineage, then it refers to this column.
        """
        ordered_columns: list[int]
        """A list of output columns (identified by index) that the rows are guaranteed to be sorted by,
        or empty if no ordering is guaranteed.
        """
        ordered_asc: list[bool]
        """Whether the ordering according to the corresponding column in ``ordered_columns``
        is ascending (``True``) or descending (``False``).
        """
        unique_columns: set[int]
        """A (possibly empty) set of output columns (identified by index),
        where values of each is guaranteed to be unique among the rows.

        NOTE: In many cases, multiple columns together may form a combination of values
        are unique among the rows (e.g., a multi-column key for a base table).
        This property does not capture such cases.
        """

        def column_in_output(self, e: ValExpr, input_index: int = 0) -> int | None:
            """Given expression ``e``, check whether it is a reference to an output column,
            and if yes, return the column index (or ``None`` otherwise).
            If ``e`` is :class:`.RelativeColumnRef`, it will need to match the given ``input_index``.
            """
            if isinstance(e, valexpr.leaf.RelativeColumnRef):
                if e.input_index == input_index:
                    return e.column_index
            elif isinstance(e, valexpr.leaf.NamedColumnRef):
                if (column_index := valexpr.find_column_in_lineage(
                        e.table_alias, e.column_name, self.output_lineage)) is not None:
                    return column_index
            return None

        def is_ordered(self, exprs: list[ValExpr], ordered_asc_required: list[bool | None]) -> list[bool] | None:
            """Check if the output already sorted according to ``exprs`` and ``orders_asc_required``.
            If a particular entry of ``orders_asc_required`` is ``None``, either ascending or descending is okay.
            If the output is indeed sorted accordingly, return the actual ascending/descending orders for ``exprs``;
            otherwise, return ``None``.
            """
            if len(self.ordered_columns) < len(exprs):
                return None
            actual_ordered_asc: list[bool] = list()
            for expr, asc_required, i, asc in zip(exprs, ordered_asc_required,
                                                  self.ordered_columns, self.ordered_asc):
                if (asc_required is not None and asc_required != asc) or\
                    (expr_col_i := self.column_in_output(expr)) is None or\
                    expr_col_i != i:
                    return None
                actual_ordered_asc.append(asc)
            # this covers the case that self's ordering is more than needed:
            return actual_ordered_asc

        def pstr(self) -> Iterable[str]:
            """Produce a sequence of lines for pretty-printing the object.
            """
            for s in self.output_metadata.pstr():
                yield f'output metadata: {s}'
            lineages = list()
            for lineage in self.output_lineage:
                lineages.append('{' + ', '.join(f'{t}.{c}' for t, c in lineage) + '}')
            s = ', '.join(lineages)
            yield f'column lineage: ({s})'
            if len(self.ordered_columns) > 0:
                s = ', '.join('{{{}}} {}'.format(', '.join(f'{t}.{c}' for t, c in self.output_lineage[i]),
                                                 'ASC' if asc else 'DESC')\
                              for i, asc in zip(self.ordered_columns, self.ordered_asc))
                yield f'ordered by: {s}'
            if len(self.unique_columns) > 0:
                s = '}, {'.join(', '.join(f'{t}.{c}' for t, c in self.output_lineage[i]) \
                                for i in self.unique_columns)
                yield f'unique columns: {{{s}}}'
            return

        @classmethod
        def from_input(cls, input_props: 'QPop.CompiledProps', **kwargs) -> Self:
            """Construct a new properties object from ``input_props``,
            copying all properties, leaving out extraneous ones, and letting ``kwarg`` override any of them.
            """
            d = {key : input_props.__dict__.get(key, None) for key in cls.__dataclass_fields__}
            return cls(**dict(d, **kwargs))

        @classmethod
        def from_inputs(cls, left_props: 'QPop.CompiledProps', right_props: 'QPop.CompiledProps', **kwargs) -> Self:
            """Construct a new properties object from ``left_props`` and ``right_props``,
            assuming by default that we are joining the two inputs in an arbitray fashion.
            ``kwarg`` can be used to override the default assumption.
            """
            output_metadata = TableMetadata(left_props.output_metadata.column_names + right_props.output_metadata.column_names,
                                            left_props.output_metadata.column_types + right_props.output_metadata.column_types)
            output_lineage = left_props.output_lineage + right_props.output_lineage
            # by default, no output ordering can be inferred because of varying join methods:
            ordered_columns: list[int] = list()
            ordered_asc: list[bool] = list()
            # by default, no unique single column can be inferred because a row may be joined with multiple others:
            unique_columns: set[int] = set()
            return cls(**dict(dict(output_metadata = output_metadata,
                                   output_lineage = output_lineage,
                                   ordered_columns = ordered_columns,
                                   ordered_asc = ordered_asc,
                                   unique_columns = unique_columns),
                              **kwargs))

    @dataclass
    class EstimatedProps:
        """Estimated properties of the operator, available by calling :meth:`.QPop.estimated`.
        """
        stats: TableStats
        """Estimated data stats for this operator's output.
        """
        @dataclass
        class StatsInBlocks:
            self_reads: int
            """Estimated number of disk block reads performed by this operator (not including children's).
            """
            self_writes: int
            """Estimated number of disk block writes performed by this operator (not including children's).
            """
            overall: int
            """Estimated number of I/Os (block reads/writes) performed by this operator and its descendents.
            """
        blocks: StatsInBlocks
        """Estimated I/Os incurred by each :meth:`.QPop.execute` pass.
        If this operator performs some caching in the first pass to reduce the costs of subsubsequent passes,
        these estimates should refer to the I/Os in a "steady-state" pass;
        I/Os incurred in the first pass can be adjusted in :attr:`.blocks_extra_init`.
        """
        blocks_extra_init: StatsInBlocks | None = None
        """Estimated one-time *extra* I/Os incurred by the very first :meth:`.QPop.execute` pass,
        or ``None`` if the first pass isn't special.
        Here the ``overall`` attribute includes any extra I/Os incurred by descendants for this
        operator's first pass, assuming steady-state descendant passes;
        in other words, it should *not* include extra I/Os incurred by descendant first passes.
        """

        def pstr(self) -> Iterable[str]:
            """Produce a sequence of lines for pretty-printing the object.
            """
            yield f'estimated I/Os = {self.blocks.overall}, ' +\
                f'with {self.blocks.self_reads} reads / {self.blocks.self_writes} writes by this op'
            if self.blocks_extra_init is not None:
                f'first-pass extra I/Os = {self.blocks_extra_init.overall}, ' +\
                f'with {self.blocks_extra_init.self_reads} reads / {self.blocks_extra_init.self_writes} writes by this op'
            for s in self.stats.pstr():
                yield f'{s}'
            return

    @final
    def __post_init__(self) -> None:
        self.compiled
        # NOTE: do NOT automatically trigger estimated by default:
        # there are situations where we don't need query planning, and
        # :class:`.StatsManager` may in fact use some hand-crafted plans, so triggering this will cause an infinite loop.
        return

    @abstractmethod
    def children(self) -> tuple['QPop[QPop.CompiledProps]', ...]:
        """Return this operator's child operators.
        """
        pass

    def pstr_more(self) -> Iterable[str]:
        """Pretty-print additional information not already covered by :meth:`.QPop.pstr`.
        Subclasses should override this method as needed.
        """
        yield from ()
        return

    @final
    def pstr(self, indent: int = 0) -> Iterable[str]:
        prefix = '' if indent == 0 else '    ' * (indent-1) + '\\___'
        yield f'{prefix}{ANSI.EMPH}{type(self).__name__}{ANSI.END}[{hex(id(self))}]'
        prefix = '    ' * indent + '| '
        for s in self.pstr_more():
            yield f'{prefix} {s}'
        if 'compiled' in self.__dict__: # test without triggering compiled()
            yield f'{prefix}{ANSI.DEMPH}{ANSI.UNDERLINE}compiled:{ANSI.END}'
            for s in self.compiled.pstr():
                yield f'{prefix}{ANSI.DEMPH} {s}{ANSI.END}'
        if 'estimated' in self.__dict__: # test without triggering compiled()
            yield f'{prefix}{ANSI.DEMPH}{ANSI.UNDERLINE}estimated:{ANSI.END}'
            for s in self.estimated.pstr():
                yield f'{prefix}{ANSI.DEMPH} {s}{ANSI.END}'
        for c in self.children():
            for s in c.pstr(indent+1):
                yield s
        return

    @cached_property
    @abstractmethod
    def compiled(self) -> P:
        """Compile (from scratch) the plan rooted at this operator and get ready for execution.
        Return a dictionary of compiled properties.
        The result will be cached (to invalidate the cache and force recompilation, see :meth:`.void_cached_props`).
        """
        pass

    @cached_property
    @abstractmethod
    def estimated(self) -> EstimatedProps:
        raise NotImplementedError

    @cached_property
    @final
    def estimated_cost(self) -> int:
        """Calculate the total estimated number of I/Os, assuming that
        this operator is the plan root and we will do one and only complete :meth:`.QPop.execute` pass.
        Besides ``estimated.blocks.overall``,
        this method will also account for all extra init cost incurred by operators in this plan
        (and will not overcount if the plan is a DAG).
        """
        extra_init_objects: set[QPop.EstimatedProps.StatsInBlocks] = set()
        self._estimated_cost_helper(extra_init_objects)
        extra_init_total = sum(extra.overall for extra in extra_init_objects)
        return extra_init_total + self.estimated.blocks.overall

    def _estimated_cost_helper(self, extra_init_objects: set[EstimatedProps.StatsInBlocks]) -> None:
        if self.estimated.blocks_extra_init in extra_init_objects:
            # already visited this subtree; skip:
            return
        for child in self.children():
            # collect from all children:
            child._estimated_cost_helper(extra_init_objects)
        if self.estimated.blocks_extra_init is not None:
            extra_init_objects.add(self.estimated.blocks_extra_init)
        return

    @abstractmethod
    def memory_blocks_required(self) -> int:
        """Return the number of memory blocks required by this operator.
        """
        pass

    @final
    def total_memory_blocks_required(self) -> int:
        """Return the total number of memory blocks required by the plan rooted at this operator.
        """
        total = self.memory_blocks_required()
        for c in self.children():
            total += c.total_memory_blocks_required()
        return total

    @final
    def void_cached_props(self, shallow: bool = False) -> None:
        """Invalidate any previously computed and cached properties of this operator,
        so that the next access to them will trigger recomputation.
        Unless ``shallow`` is set to ``True``, all descendent properties will be invalidated too.
        """
        del self.compiled
        del self.estimated
        if not shallow: # recursively invalidate the entire tree
            for c in self.children():
                c.void_cached_props()
        return

    def column_in_output(self, e: ValExpr) -> int | None:
        if isinstance(e, valexpr.leaf.RelativeColumnRef):
            return e.column_index
        elif isinstance(e, valexpr.leaf.NamedColumnRef):
            if (column_index := valexpr.find_column_in_lineage(
                    e.table_alias, e.column_name, self.compiled.output_lineage)) is not None:
                return column_index
        return None

    def column_in_children(self, e: ValExpr) -> tuple[int, int] | None:
        for i in range(len(self.children())):
            if (column_index := self.column_in_child(e, i)) is not None:
                return i, column_index
        return None

    def column_in_child(self, e: ValExpr, child_index: int) -> int | None:
        if isinstance(e, valexpr.leaf.RelativeColumnRef):
            if child_index == e.input_index:
                return e.column_index
        elif isinstance(e, valexpr.leaf.NamedColumnRef):
            if (column_index := valexpr.find_column_in_lineage(
                    e.table_alias, e.column_name, self.children()[child_index].compiled.output_lineage)) is not None:
                return column_index
        return None

    def compile_valexpr(self, e: ValExpr, row_vars: list[str] = ['row0', 'row1']) -> tuple[str, Any]:
        output_lineages: list[OutputLineage] = [c.compiled.output_lineage for c in self.children()]
        code = valexpr.to_code_str(e, output_lineages, row_vars)
        exec = compile(code, '<string>', 'eval')
        return code, exec

    @abstractmethod
    def execute(self) -> Generator[tuple, None, None]:
        """Return a Python generator that executes the operator and iterates over the result rows.
        """
        pass

    @dataclass
    class Sarg:
        """A data structure representing arguments for a range search.
        In genernal, ``key_*`` attributes may be literal values or expressions whose values will be computed at run time.
        """
        is_range: bool | None
        """Whether it's a range search.
        If not, it's searching for a specific key, so ``key_*`` should be the same
        and ``*_exclusive`` should be ``False``.
        """
        key_lower: ValExpr | None
        key_upper: ValExpr | None
        lower_exclusive: bool | None
        upper_exclusive: bool | None

        def to_str(self) -> str:
            return '{}{}, {}{}'.format(
                '(' if self.lower_exclusive is None or self.lower_exclusive else '[',
                None if self.key_lower is None else self.key_lower.to_str(),
                None if self.key_upper is None else self.key_upper.to_str(),
                ')' if self.upper_exclusive is None or self.upper_exclusive else ']')

class CPop(Pop):    
    @abstractmethod
    def execute(self) -> str:
        """Execute the command and return a response or throw an exception.
        """
        pass

    def pstr_more(self) -> Iterable[str]:
        """Pretty-print additional information not already covered by :meth:`.CPop.pstr`.
        Subclasses should override this method as needed.
        """
        yield from ()
        return

    @final
    def pstr(self, indent: int = 0) -> Iterable[str]:
        prefix = '' if indent == 0 else '    ' * (indent-1) + '\\___'
        yield f'{prefix}{ANSI.EMPH}{type(self).__name__}{ANSI.END}[{hex(id(self))}]'
        prefix = '    ' * indent + '| '
        for s in self.pstr_more():
            yield f'{prefix} {s}'
        return
