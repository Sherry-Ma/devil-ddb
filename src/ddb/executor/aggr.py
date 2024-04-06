from typing import cast, Final, Iterable, Generator, Sequence, Any
from dataclasses import dataclass
from functools import cached_property, partial

from ..profile import profile_generator
from ..storage import HeapFile
from ..validator import valexpr, ValExpr, OutputLineage
from ..validator.valexpr.eval import *
from ..metadata import TableMetadata, INTERNAL_ANON_COLUMN_NAME_FORMAT, INTERNAL_ANON_TABLE_NAME_FORMAT

from .interface import ExecutorException, QPop
from .util import ExtSortBuffer

class AggrPop(QPop['AggrPop.CompiledProps']):
    """A physical operator for computing aggregate expression values over grouped input rows.
    This operator will output one row for each group, containing only the group-by values
    followed by the aggregate values.
    The input rows must have already been grouped such that all rows in the same group appear consecutively.
    For any aggregate that is not incrementally computable,
    this operator uses extra memory and temporary files as needed to sort all input values in the group.
    """

    @dataclass
    class CompiledProps(QPop.CompiledProps):
        groupby_codes: list[str]
        """Python code for each GROUP BY expression.
        """
        groupby_execs: list[Any]
        """Python executable for each GROUP BY expression.
        """
        aggr_input_codes: list[str]
        """Python code for computing an input for each aggregate expression from an input row.
        """
        aggr_input_execs: list[Any]
        """Python executable for computing an input for each aggregate expression from an input row.
        """
        aggr_init_codes: list[str]
        """Python code for computing the initial state for each aggregate expression.
        """
        aggr_init_execs: list[Any]
        """Python executable for computing the initial state for each aggregate expression.
        """
        aggr_add_codes: list[str]
        """Python code for computing the updated state (upon receving an input) for each aggregate expression.
        """
        aggr_add_execs: list[str]
        """Python executable for computing the updated state (upon receving an input) for each aggregate expression.
        """
        aggr_finalize_codes: list[str]
        """Python code for computing the final result for each aggregate expression.
        """
        aggr_finalize_execs: list[str]
        """Python executable for computing the final result for each aggregate expression.
        """

        def pstr_more_compiled(self) -> Iterable[str]:
            yield from super().pstr()
            yield f'group by {len(self.groupby_codes)} expressions:'
            for column_name, code in zip(self.output_metadata.column_names, self.groupby_codes):
                yield f'  {column_name}: {code}'
            yield f'{len(self.aggr_add_codes)} aggregate expressions:'
            for column_name, code in zip(self.output_metadata.column_names[len(self.groupby_codes):], self.aggr_add_codes):
                yield f'  {column_name}: {code}'
            return

    def __init__(self, input: QPop[QPop.CompiledProps],
                 groupby_exprs: list[ValExpr],
                 aggr_exprs: list[valexpr.AggrValExpr],
                 column_names: Sequence[str | None] | None,
                 num_memory_blocks: int) -> None:
        """Construct a aggregation operator on top of the given ``input``.
        """
        super().__init__(input.context)
        self.input: Final = input
        self.output_table_name: Final[str] = INTERNAL_ANON_TABLE_NAME_FORMAT.format(pop=type(self).__name__, hex=hex(id(self)))
        self.groupby_exprs = groupby_exprs
        self.aggr_exprs = aggr_exprs
        self.output_column_names: Final[list[str]] = list()
        for i, (expr, column_name) in enumerate(zip(
            self.groupby_exprs + cast(list[ValExpr], self.aggr_exprs),
            [None] * (len(self.groupby_exprs) + len(self.aggr_exprs)) if column_names is None else column_names)):
            if column_name is not None:
                self.output_column_names.append(column_name)
            elif isinstance(expr, valexpr.leaf.NamedColumnRef):
                self.output_column_names.append(expr.column_name)
            else:
                self.output_column_names.append(INTERNAL_ANON_COLUMN_NAME_FORMAT.format(index = i)) # default
        self.num_memory_blocks: Final = num_memory_blocks
        self.num_non_incremental: Final = sum(not aggr.is_incremental() for aggr in self.aggr_exprs)
        if self.num_memory_blocks < 3 * self.num_non_incremental:
            raise ExecutorException('aggregation needs at least 3 memory blocks for merge sort')
        return

    def memory_blocks_required(self) -> int:
        return self.num_memory_blocks

    def children(self) -> tuple[QPop[QPop.CompiledProps], ...]:
        return (self.input, )

    def pstr_more(self) -> Iterable[str]:
        yield f'AS {self.output_table_name}:'
        for expr, name in zip(self.groupby_exprs + cast(list[ValExpr], self.aggr_exprs), self.output_column_names):
            yield f'  {name}: {expr.to_str()}'
        return

    @cached_property
    def compiled(self) -> 'AggrPop.CompiledProps':
        input_props = self.input.compiled
        output_column_types = [e.valtype() for e in self.groupby_exprs + self.aggr_exprs]
        output_lineage: OutputLineage = list()
        preserved_input_columns: dict[int, int] = dict()
        i: int | None
        for i, (expr, output_column_name) in enumerate(zip(
            self.groupby_exprs + cast(list[ValExpr], self.aggr_exprs),
            self.output_column_names)):
            output_column_lineage = set(((self.output_table_name, output_column_name), ))
            if (input_column_index := self.column_in_child(expr, 0)) is not None:
                output_column_lineage = output_column_lineage | input_props.output_lineage[input_column_index]
                preserved_input_columns[input_column_index] = i
            output_lineage.append(output_column_lineage)
        ordered_columns: list[int] = list()
        ordered_asc: list[bool] = list()
        for input_column_index, asc in zip(input_props.ordered_columns, input_props.ordered_asc):
            if (i := preserved_input_columns.get(input_column_index)) is not None:
                ordered_columns.append(i)
                ordered_asc.append(asc)
            else: # any "gap" means remaining columns won't be ordered
                break
        unique_columns: set[int] = set()
        for input_column_index in input_props.unique_columns:
            if (i := preserved_input_columns.get(input_column_index)) is not None:
                unique_columns = unique_columns | {i}
        # grouping will enforce uniqueness for the group-by columns as a whole,
        # but unfortunately we only capture single-column uniqueness:
        if len(self.groupby_exprs) == 1:
            unique_columns = unique_columns | {0}
        # compile!
        # GROUP BY expressions and inputs to aggregates are just compiled in the generic way:
        groupby_codes: list[str] = list()
        groupby_execs: list[Any] = list()
        aggr_input_codes: list[str] = list()
        aggr_input_execs: list[Any] = list()
        for exprs, codes, execs in ((self.groupby_exprs, groupby_codes, groupby_execs),
                                    ((aggr_expr.children()[0] for aggr_expr in self.aggr_exprs),
                                     aggr_input_codes, aggr_input_execs)):
            for expr in exprs:
                code, exec = self.compile_valexpr(expr)
                codes.append(code)
                execs.append(exec)
        # aggregates themselves are compiled differently:
        aggr_init_codes: list[str] = list()
        aggr_add_codes: list[str] = list()
        aggr_finalize_codes: list[str] = list()
        for e in self.aggr_exprs:
            aggr_init_codes.append(e.code_str_init())
            aggr_add_codes.append(e.code_str_add(f'state', 'new_val'))
            aggr_finalize_codes.append(e.code_str_finalize(f'state'))
        aggr_init_execs: list[Any] = list()
        aggr_add_execs: list[Any] = list()
        aggr_finalize_execs: list[Any] = list()
        for codes, execs in ((aggr_init_codes, aggr_init_execs),
                             (aggr_add_codes, aggr_add_execs),
                             (aggr_finalize_codes, aggr_finalize_execs)):
            for code in codes:
                execs.append(compile(code, '<string>', 'eval'))
        return AggrPop.CompiledProps(
            output_metadata = TableMetadata(self.output_column_names, output_column_types),
            output_lineage = output_lineage,
            ordered_columns = ordered_columns,
            ordered_asc = ordered_asc,
            unique_columns = unique_columns,
            groupby_codes = groupby_codes,
            groupby_execs = groupby_execs,
            aggr_input_codes = aggr_input_codes,
            aggr_input_execs = aggr_input_execs,
            aggr_init_codes = aggr_init_codes,
            aggr_init_execs = aggr_init_execs,
            aggr_add_codes = aggr_add_codes,
            aggr_add_execs = aggr_add_execs,
            aggr_finalize_codes = aggr_finalize_codes,
            aggr_finalize_execs = aggr_finalize_execs)

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        stats = self.context.zm.grouping_stats(
            self.input.estimated.stats,
            [cast(ValExpr, valexpr.relativize(e, [self.input.compiled.output_lineage]))
             for e in self.groupby_exprs],
            [cast(valexpr.AggrValExpr, valexpr.relativize(a, [self.input.compiled.output_lineage]))
             for a in self.aggr_exprs])
        return QPop.EstimatedProps(
            stats = stats,
            blocks = QPop.StatsInBlocks(
                self_reads = 0,
                self_writes = 0,
                overall = self.input.estimated.blocks.overall))
    
    def _tmp_file_create(self, aggr_i: int, level: int, run: int) -> HeapFile:
        """Create a temporary file for a result run in a given level with an ordinal run number,
        used for computing non-incremental ``self.aggr_exprs[aggr_i]``.
        Levels start at ``0`` (results of initial sorting pass) and go up by one with each additional merge pass.
        Each level may contain multiple result runs, numbered from ``0``.
        The file name is chosen in a way to help deduce which ``Pop`` produced it and what level and run number it has.
        """
        f = self.context.sm.heap_file(
            self.context.tmp_tx,
            f'.tmp-{hex(id(self))}-{aggr_i}-{level}-{run}', [], create_if_not_exists=True)
        f.truncate()
        return f

    def _tmp_file_delete(self, run: HeapFile) -> None:
        """Delete a temporary file for a result run.
        """
        self.context.sm.delete_heap_file(self.context.tmp_tx, run.name)
        return

    @staticmethod
    def _compare(this: tuple, that: tuple) -> int:
        if this < that:
            return -1
        elif this == that:
            return 0
        else:
            return 1

    def _finalize_group(self, sort_buffers: list[ExtSortBuffer|None], aggr_states: list) -> tuple:
        # process inputs for non-incremental aggregates first:
        for i, (buffer, exec) in enumerate(zip(sort_buffers, self.compiled.aggr_add_execs)):
            if buffer is not None:
                state = aggr_states[i]
                for row in cast(ExtSortBuffer, buffer).iter_and_clear():
                    new_val = row[0] if len(row) == 1 else row
                    state = eval(exec, None, dict(state = state, new_val = new_val))
                aggr_states[i] = state
        # finalize everything:
        return tuple(eval(exec, None, dict(state=state))
                     for exec, state
                     in zip(self.compiled.aggr_finalize_execs, aggr_states))

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        sort_buffers: list[ExtSortBuffer|None] = [ # for non-incremental aggregates
            None if aggr_expr.is_incremental() else
            ExtSortBuffer(self._compare,
                          partial(self._tmp_file_create, i), self._tmp_file_delete,
                          self.num_memory_blocks // self.num_non_incremental,
                          deduplicate=aggr_expr.is_distinct)
            for i, aggr_expr in enumerate(self.aggr_exprs)
        ]
        cur_groupby_vals: tuple|None = None
        aggr_states: list = list()
        if len(self.groupby_exprs) == 0: # special handling when there is no GROUP BY; automatically start the group
            cur_groupby_vals = ()
            aggr_states = [eval(exec, None) for exec in self.compiled.aggr_init_execs]
        for row in self.input.execute():
            groupby_vals = tuple(eval(exec, None, dict(row0 = row))
                                 for exec in self.compiled.groupby_execs)
            aggr_inputs = tuple(eval(exec, None, dict(row0 = row))
                                for exec in self.compiled.aggr_input_execs)
            if groupby_vals != cur_groupby_vals:
                if cur_groupby_vals is not None:
                    # end the last group and finalize the results
                    final_aggr_vals = self._finalize_group(sort_buffers, aggr_states)
                    yield (*cur_groupby_vals, *final_aggr_vals)
                # start new group:
                cur_groupby_vals = groupby_vals
                aggr_states = [eval(exec, None) for exec in self.compiled.aggr_init_execs]
            for i, (buffer, exec, aggr_input) in enumerate(zip(sort_buffers, self.compiled.aggr_add_execs, aggr_inputs)):
                if buffer is None: # incrementally computable; just update state
                    aggr_states[i] = eval(exec, None, dict(state = aggr_states[i], new_val = aggr_input))
                else: # not incrementally computable; just add to sort buffer
                    row = aggr_input if isinstance(aggr_input, tuple) else (aggr_input, )
                    buffer.add(row)
        if cur_groupby_vals is not None:
            # end the last group and finalize the results:
            final_aggr_vals = self._finalize_group(sort_buffers, aggr_states)
            yield (*cur_groupby_vals, *final_aggr_vals)
        return
