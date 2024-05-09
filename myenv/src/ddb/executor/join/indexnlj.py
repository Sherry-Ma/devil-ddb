from typing import cast, Final, Iterable, Generator, Any
from dataclasses import dataclass
from functools import cached_property

from ...profile import profile_generator
from ...validator import ValExpr, valexpr
from ...validator.valexpr.eval import *

from ..interface import QPop
from ..indexscan import IndexScanPop

from .interface import JoinPop

class IndexNLJoinPop(JoinPop['IndexNLJoinPop.CompiledProps']):
    """Index nested-loop join physical operator,
    which streams tuples produced by the left operator and probes the index associated with the right operator.
    No buffering is performed by this operator so it doesn't use any memory blocks.
    """

    @dataclass
    class CompiledProps(QPop.CompiledProps):
        key_lower_code: str | None
        """Python code for computing the lower bound for index search.
        """
        key_lower_exec: Any
        """Python executable for computing the lower bound for index search.
        """
        key_upper_code: str | None
        """Python code for computing the upper bound for index search.
        """
        key_upper_exec: Any
        """Python executable for computing the upper bound for index search.
        """
        cond_code: str | None
        """Python code for the extra join condition.
        """
        cond_exec: Any
        """Python executable for the extra join condition.
        """

        def pstr(self) -> Iterable[str]:
            yield from super().pstr()
            for k, key in (('key_lower_code', 'lower bound code'),
                           ('key_upper_code', 'upper bound code'),
                           ('cond_code', 'extra join condition code')):
                if (v := self.__dict__.get(k)) is not None:
                    yield f'{key}: {v}'
            return

    def __init__(self, left: QPop[QPop.CompiledProps], right: IndexScanPop, sarg: QPop.Sarg, cond: ValExpr | None) -> None:
        """Construct a index nested-loop join between ``left`` and ``right`` inputs.
        ``sarg`` contains expressions to be evaluated for each output row of the left operator
        to obtain the search key range for the inner operator.
        ``cond`` is an optional join condition that will be additionally applied.
        """
        super().__init__(left, right)
        self.sarg: Final = sarg
        self.cond: Final = cond
        return

    def memory_blocks_required(self) -> int:
        return 0

    def pstr_more(self) -> Iterable[str]:
        yield 'probe right using: ' + self.sarg.to_str()
        if self.cond is not None:
            yield 'extra join condition: ' + self.cond.to_str()
        return

    def _infer_ordering_uniqueness_props(self) -> tuple[list[int], list[bool], set[int]]:
        left_props = self.left.compiled
        right_props = self.right.compiled
        ordered_columns: list[int] = left_props.ordered_columns # at least the left ordering will be preserved
        ordered_asc: list[bool] = left_props.ordered_asc
        unique_columns: set[int] = set()
        # see if we can infer more ordering/uniqueness; only chance is that we don't have a range scan on inner.
        # we look for the case where the lookup key is a column from outer:
        if self.sarg.key_lower is not None and self.sarg.key_upper is not None and\
            (left_col_i := self.column_in_child(self.sarg.key_lower, 0)) is not None and\
            left_col_i == self.column_in_child(self.sarg.key_upper, 0):
            col_i_offset = len(left_props.output_metadata.column_names)
            if len(left_props.ordered_columns) == 1 and left_props.ordered_columns[0] == left_col_i:
                # final output ordering can be further extended by right ordering:
                ordered_columns = ordered_columns.copy()
                ordered_asc = ordered_asc.copy()
                for right_col_i, asc in zip(right_props.ordered_columns, right_props.ordered_asc):
                    ordered_columns.append(col_i_offset + right_col_i)
                    ordered_asc.append(asc)
            if left_col_i in left_props.unique_columns:
                if cast(IndexScanPop, self.right).is_by_row_id() or cast(IndexScanPop, self.right).is_by_primary_key():
                    # join key is unique in both left and right, so right uniqueness is preserved too:
                    unique_columns = unique_columns | set(col_i_offset + right_col_i for right_col_i in right_props.unique_columns)
                else: # right is a secondary (not necessarily unique) index scan
                    # one left row may join with multiple right rows, so left uniqueness is destroyed:
                    right_row_id_index = 1
                    unique_columns = { col_i_offset + right_row_id_index }
        return ordered_columns, ordered_asc, unique_columns

    @cached_property
    def compiled(self) -> 'IndexNLJoinPop.CompiledProps':
        ordered_columns, ordered_asc, unique_columns = self._infer_ordering_uniqueness_props()
        key_lower_code, key_lower_exec = self.compile_valexpr(self.sarg.key_lower) if self.sarg.key_lower is not None else (None, None)
        key_upper_code, key_upper_exec = self.compile_valexpr(self.sarg.key_upper) if self.sarg.key_upper is not None else (None, None)
        cond_code, cond_exec = self.compile_valexpr(self.cond) if self.cond is not None else (None, None)
        return IndexNLJoinPop.CompiledProps.from_inputs(self.left.compiled, self.right.compiled,
                                                        ordered_columns = ordered_columns,
                                                        ordered_asc = ordered_asc,
                                                        unique_columns = unique_columns,
                                                        key_lower_code = key_lower_code,
                                                        key_lower_exec = key_lower_exec,
                                                        key_upper_code = key_upper_code,
                                                        key_upper_exec = key_upper_exec,
                                                        cond_code = cond_code,
                                                        cond_exec = cond_exec)

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        # since estimates on the right is for each left row,
        # a cross-product estimate is exactly what we need for sarg;
        # just need to add the extra post-condition if needed:
        stats = self.context.zm.join_stats(
            self.left.estimated.stats,
            self.right.estimated.stats,
            None if self.cond is None else\
                valexpr.relativize(self.cond, [self.left.compiled.output_lineage, self.right.compiled.output_lineage]))
        return QPop.EstimatedProps(
            stats = stats,
            blocks = QPop.EstimatedProps.StatsInBlocks(
                self_reads = 0,
                self_writes = 0,
                overall = self.left.estimated.blocks.overall +\
                    self.left.estimated.stats.row_count * self.right.estimated.blocks.overall))

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        key_lower_exec = self.compiled.key_lower_exec
        key_upper_exec = self.compiled.key_upper_exec
        cond_exec = self.compiled.cond_exec
        right = cast(IndexScanPop, self.right)
        for outer_row in self.left.execute():
            key_lower = None if key_lower_exec is None else eval(key_lower_exec, None, dict(row0 = outer_row))
            key_upper = None if key_upper_exec is None else eval(key_upper_exec, None, dict(row0 = outer_row))
            right.set_range(key_lower, key_upper,
                            self.sarg.lower_exclusive, self.sarg.upper_exclusive)
            for inner_row in right.execute():
                if cond_exec is None or eval(cond_exec, None, dict(row0 = outer_row, row1 = inner_row)):
                    yield (*outer_row, *inner_row)
        return