from typing import cast, Final, Iterable, Generator, Any
from contextlib import closing
from dataclasses import dataclass
from functools import cached_property
from math import ceil

from ...globals import BLOCK_SIZE
from ...profile import profile_generator
from ...validator import ValExpr, valexpr
from ...validator.valexpr.eval import *
from ...storage import HeapFile

from ..util import BufferedReader, BufferedWriter
from ..interface import QPop

import time
from .interface import JoinPop

class MergeEqJoinPop(JoinPop['MergeEqJoinPop.CompiledProps']):
    """Merge equijoin physical operator.
    It assumes both inputs are already sorted and will be streamed in efficiently.
    Normally this operator will be not performs any buffering itself,
    but in cases when mutiple rows from both inputs join and a mini nested loop is required,
    it may use up to two memory blocks and may spill to tmp storage.
    """

    @dataclass
    class CompiledProps(QPop.CompiledProps):
        cmp_code: str
        """Python code for comparing left and right rows.
        """
        cmp_exec: Any
        """Python executable for comparing left and right rows.
        """
        side_eq_codes: list[str]
        """Python code for checking if two rows from the same input have identical join key values.
        The first entry is for the left input and second for the right.
        """
        side_eq_execs: list[Any]
        """Python executable for checking if two rows from the same input have identical join key values.
        The first entry is for the left input and second for the right.
        """

        def pstr(self) -> Iterable[str]:
            yield from super().pstr()
            yield 'left/right row comparison code: ' + self.cmp_code
            for i, side_eq_code in enumerate(self.side_eq_codes):
                yield 'same-input join key equality code (' + ('left' if i == 0 else 'right') + '): ' + side_eq_code
            return

    def __init__(self, left: QPop[QPop.CompiledProps], right: QPop[QPop.CompiledProps],
                 left_exprs: list[ValExpr],
                 right_exprs: list[ValExpr],
                 orders_asc: list[bool]) -> None:
        """Construct a merge join between ``left`` and ``right`` inputs on the specified expressions
        (most commonly columns): ``left_exprs`` and ``right_exprs`` are to be evaluated over each row
        from left input and each row from right input, respectively.
        We assume that both inputs are already sorted according to the specified expressions
        and in the specified ascending/descending orders.
        """
        super().__init__(left, right)
        self.left_exprs: Final = left_exprs
        self.right_exprs: Final = right_exprs
        self.orders_asc: Final = orders_asc

    def memory_blocks_required(self) -> int:
        return 2

    def pstr_more(self) -> Iterable[str]:
        for left_expr, right_expr, asc in zip(self.left_exprs, self.right_exprs, self.orders_asc):
            yield f'{left_expr.to_str()} = {right_expr.to_str()} ' + ('ASC' if asc else 'DESC')
        return

    def _infer_ordering_uniqueness_props(self) -> tuple[list[int], list[bool], set[int]]:
        left_props = self.left.compiled
        right_props = self.right.compiled
        # first, let's figure out how merge-join columns map to columns of children:
        ordered_columns_in_children: tuple[list[int], list[int]] = (list(), list())
        ordered_asc_in_children: tuple[list[bool], list[bool]] = (list(), list())
        for child_i in range(len(self.children())):
            exprs = self.left_exprs if child_i == 0 else self.right_exprs
            for expr, asc in zip(exprs, self.orders_asc):
                if (col_i := self.column_in_child(expr, child_i)) is not None:
                    ordered_columns_in_children[child_i].append(col_i)
                    ordered_asc_in_children[child_i].append(asc)
                else: # merge-joining by something that's not an output column
                    break # this gap would destroy the rest of the ordering
        # baseline: the ordering of join columns is safe, but in general,
        # ordering beyond these as well as uniqueness will be destroyed
        # because of mini nested-loop join for rows with same join values.
        # (as a convention, when two columns are equal, we prefer using the earlier one for specifying ordering.
        # TODO: this is not ideal; consider track equivalence classes of columns in the future.)
        ordered_columns: list[int] = ordered_columns_in_children[0]
        ordered_asc: list[bool] = ordered_asc_in_children[0]
        unique_columns: set[int] = set()
        # let's still try to catch some (hopefully common) cases where more properties can be inferred.
        left_is_nice =\
            len(ordered_columns_in_children[0]) == len(self.left_exprs) and\
            all(i in left_props.unique_columns for i in ordered_columns_in_children[0])
        right_is_nice =\
            len(ordered_columns_in_children[1]) == len(self.right_exprs) and\
            all(i in right_props.unique_columns for i in ordered_columns_in_children[1])
        col_i_offset = len(left_props.output_metadata.column_names)
        if left_is_nice:
            # one-many or one-one join.
            # all left ordering is preserved and can be extended with right ordering:
            ordered_columns = left_props.ordered_columns.copy()
            ordered_asc = left_props.ordered_asc.copy()
            for col_i, asc in zip(right_props.ordered_columns, right_props.ordered_asc):
                if col_i in ordered_columns_in_children[1]:
                    continue # ignore join columns, already specified with left
                ordered_columns.append(col_i_offset + col_i)
                ordered_asc.append(asc)
            # all right uniqueness is also preserved:
            unique_columns = set(col_i_offset + col_i for col_i in right_props.unique_columns)
            # left uniqueness is also perserved if join is additionally one-one:
            if right_is_nice:
                unique_columns = unique_columns | left_props.unique_columns
        elif right_is_nice: # not right_is_nice is implied
            # many-one join.
            # all right ordering is preserved and can be extended with left ordering:
            for col_i, asc in zip(right_props.ordered_columns, right_props.ordered_asc):
                if col_i in ordered_columns_in_children[1]:
                    continue # ignore join columns, already specified with left previously
                ordered_columns.append(col_i_offset + col_i)
                ordered_asc.append(asc)
            for col_i, asc in zip(left_props.ordered_columns, left_props.ordered_asc):
                if col_i in ordered_columns_in_children[0]:
                    continue # already specified previously
                ordered_columns.append(col_i)
                ordered_asc.append(asc)
            # all left uniqueness is also perserved:
            unique_columns = left_props.unique_columns
        return ordered_columns, ordered_asc, unique_columns

    def compare(self, this: tuple, that: tuple) -> int:
        """Compare two rows ``this`` and ``that``,
        and return ``-1``, ``0``, or ``1`` if ``this`` is less than (i.e., goes before in ascending order),
        equal to, or greater than ``that``, respectively.
        """
        return eval(self.compiled.cmp_exec, None, dict(this=this, that=that))

    def _compile_comparators(self) -> tuple[str, Any, list[str], list[Any]]:
        # construct the comparator for merging:
        this_before_that_codes: list[str] = list()
        eq_codes: list[str] = list()
        for left_expr, right_expr, asc in zip(self.left_exprs, self.right_exprs, self.orders_asc):
            op = '<' if asc else '>'
            this_code, _ = self.compile_valexpr(left_expr, ['this', 'that'])
            that_code, _ = self.compile_valexpr(right_expr, ['this', 'that'])
            this_before_that_code = f'{this_code}{op}{that_code}'
            if len(eq_codes) > 0:
                this_before_that_code = ' and '.join(eq_codes) + ' and ' + this_before_that_code
            this_before_that_codes.append(this_before_that_code)
            eq_codes.append(f'{this_code}=={that_code}')
        this_before_that_code = '(' + ') or ('.join(this_before_that_codes) + ')'
        eq_code = ' and '.join(eq_codes)
        cmp_code = f'-1 if {this_before_that_code} else (0 if ({eq_code}) else 1)'
        cmp_exec = compile(cmp_code, '<string>', 'eval')
        # construct the comparators for equality condition for each input (to find the batch that all join):
        side_eq_codes: list[str] = list()
        side_eq_execs: list[Any] = list()
        for child, exprs in ((self.left, self.left_exprs), (self.right, self.right_exprs)):
            side_codes: list[str] = list()
            for expr in exprs:
                this_code, _ = self.compile_valexpr(expr, ['this', 'this'])
                that_code, _ = self.compile_valexpr(expr, ['that', 'that'])
                side_codes.append(f'{this_code}=={that_code}')
            side_eq_code = ' and '.join(side_codes)
            side_eq_exec = compile(side_eq_code, '<string>', 'eval')
            side_eq_codes.append(side_eq_code)
            side_eq_execs.append(side_eq_exec)
        return cmp_code, cmp_exec, side_eq_codes, side_eq_execs

    @cached_property
    def compiled(self) -> 'MergeEqJoinPop.CompiledProps':
        ordered_columns, ordered_asc, unique_columns = self._infer_ordering_uniqueness_props()
        cmp_code, cmp_exec, side_eq_codes, side_eq_execs = self._compile_comparators()
        return MergeEqJoinPop.CompiledProps.from_inputs(self.left.compiled, self.right.compiled,
                                                        ordered_columns = ordered_columns,
                                                        ordered_asc = ordered_asc,
                                                        unique_columns = unique_columns,
                                                        cmp_code = cmp_code,
                                                        cmp_exec = cmp_exec,
                                                        side_eq_codes = side_eq_codes,
                                                        side_eq_execs = side_eq_execs)

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        relativized_equalities = [
            cast(ValExpr, valexpr.relativize(
                valexpr.binary.EQ(e1, e2),
                [self.left.compiled.output_lineage, self.right.compiled.output_lineage]))
            for e1, e2 in zip(self.left_exprs, self.right_exprs)
        ]
        stats = self.context.zm.join_stats(
            self.left.estimated.stats,
            self.right.estimated.stats,
            valexpr.make_conjunction(relativized_equalities))
        # make some guess about how many reads/writes are needed by mini nested loops:
        # it's not how our algorithm operates, but as an estimate it's fine.
        # assume left is the bigger input:
        joining_per_left_row = ceil(stats.row_count / max(self.left.estimated.stats.row_count, 1))
        joining_blocks_per_left_row = ceil(
            joining_per_left_row * self.right.estimated.stats.row_size
            / BLOCK_SIZE)
        extra_ios = max(joining_blocks_per_left_row - 1, 0) * joining_per_left_row
        return QPop.EstimatedProps(
            stats = stats,
            blocks = QPop.EstimatedProps.StatsInBlocks(
                self_reads = extra_ios,
                self_writes = extra_ios,
                overall = self.left.estimated.blocks.overall + self.right.estimated.blocks.overall + 2*extra_ios))

    def mini_bnlcj_execute(self, writer0: BufferedWriter, writer1: BufferedWriter) -> Generator[tuple, None, None]:
        def source(i):
            if i == 0:
                return [writer0.buffer] if writer0.num_blocks_flushed == 0\
                    else BufferedReader(1).iter_buffer(writer0.file.iter_scan())
            else:
                return [writer1.buffer] if writer1.num_blocks_flushed == 0\
                    else BufferedReader(1).iter_buffer(writer1.file.iter_scan())
        reverse = writer0.num_blocks_flushed > writer1.num_blocks_flushed
        for outer_buffer in (source(1) if reverse else source(0)):
            for inner_buffer in (source(0) if reverse else source(1)):
                for outer_row in outer_buffer:
                    for inner_row in inner_buffer:
                        if reverse:
                            yield (*inner_row, *outer_row)
                        else:
                            yield (*outer_row, *inner_row)
        return

    def mini_bjlcj_prepare(self,
                           starting_row0: tuple, iter0: Generator[tuple, None, None], file0: HeapFile,
                           starting_row1: tuple, iter1: Generator[tuple, None, None], file1: HeapFile)\
        -> tuple[BufferedWriter, tuple | None, BufferedWriter, tuple | None]:
        def _helper(starting_row: tuple, iter: Generator[tuple, None, None], file: HeapFile, eq_exec)\
            -> tuple[BufferedWriter, tuple | None]:
            file.truncate()
            writer = BufferedWriter(file, 1)
            writer.write(starting_row)
            while True:
                row = next(iter, None)
                if row is None or not eval(eq_exec, None, dict(this=starting_row, that=row)):
                    if writer.num_blocks_flushed > 0:
                        # already spilled, so let's write buffered rows so join can proceed from the beginning;
                        # otherwise, no need to flush at all -- just use in-memory buffer:
                        writer.flush()
                    return writer, row
                writer.write(row)
        writer0, row0_next = _helper(starting_row0, iter0, file0, self.compiled.side_eq_execs[0])
        writer1, row1_next = _helper(starting_row1, iter1, file1, self.compiled.side_eq_execs[1])
        return writer0, row0_next, writer1, row1_next
    
    
    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        start_time = time.time()
        cmp_exec = self.compiled.cmp_exec
        with self.context.sm.heap_file(self.context.tmp_tx, f'.tmp-{hex(id(self))}-left', [], create_if_not_exists=True) as file0, \
            self.context.sm.heap_file(self.context.tmp_tx, f'.tmp-{hex(id(self))}-right', [], create_if_not_exists=True) as file1:
            # because we want more explict control over input generators, use the with-closing pattern instead of for below:
            with closing(self.left.execute()) as iter0, closing(self.right.execute()) as iter1:
                row0 = next(iter0, None)
                row1 = next(iter1, None)
                while row0 is not None and row1 is not None:
                    cmp_result = eval(cmp_exec, None, dict(this=row0, that=row1))
                    if cmp_result < 0:
                        row0 = next(iter0, None)
                    elif cmp_result > 0:
                        row1 = next(iter1, None)
                    else:
                        writer0, row0_next, writer1, row1_next = self.mini_bjlcj_prepare(row0, iter0, file0, row1, iter1, file1)
                        yield from self.mini_bnlcj_execute(writer0, writer1)
                        row0, row1 = row0_next, row1_next
                    
        end_time = time.time()
        elapsed_time = end_time - start_time

        print("Time taken:", elapsed_time, "seconds")
        print("herrrrr")
        return
