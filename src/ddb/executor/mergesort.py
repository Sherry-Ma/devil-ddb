from typing import cast, Final, Iterable, Generator, Any
from dataclasses import dataclass
from functools import cmp_to_key, cached_property
from math import ceil
import logging

from ..profile import profile_generator
from ..storage import HeapFile
from ..validator import ValExpr
from ..validator.valexpr.eval import *

from .interface import QPop, ExecutorException
from .util import BufferedReader, BufferedWriter, PQueue

class MergeSortPop(QPop['MergeSortPop.CompiledProps']):
    """External merge sort physical operator.
    It will use as many memory blocks as it is given,
    with an option to use a different number of blocks for the final pass (as it may be used to optimize subsequent join).
    The intermediate runs will be stored as heap files in the tmp space.
    """

    @dataclass
    class CompiledProps(QPop.CompiledProps):
        cmp_code: str
        """Python code for comparing rows.
        """
        cmp_exec: Any
        """Python executable for comparing rows.
        """
        sort_key: Any
        """Python comparator for built-in sort.
        """
        q_cmp: Any
        """Python comparator for priority queue.
        """

        def pstr(self) -> Iterable[str]:
            yield from super().pstr()
            if self.cmp_code is not None:
                yield 'row comparison code: ' + self.cmp_code
            return

    def __init__(self, input: QPop[QPop.CompiledProps],
                 exprs: list[ValExpr],
                 orders_asc: list[bool],
                 num_memory_blocks: int, num_memory_blocks_final: int | None) -> None:
        """Construct a sort on top of the given ``input``, using the specified expressions and orders.
        The number of memory blocks for the final pass does NOT include any block used for buffering output.
        """
        super().__init__(input.context)
        self.input: Final = input
        self.exprs: Final = exprs
        self.orders_asc: Final = orders_asc
        self.num_memory_blocks: Final = num_memory_blocks
        if self.num_memory_blocks <= 2:
            raise ExecutorException('merge sort needs at least 3 memory blocks to perform a merge')
        self.num_memory_blocks_final: Final = num_memory_blocks_final or self.num_memory_blocks
        return

    def memory_blocks_required(self) -> int:
        return max(self.num_memory_blocks, self.num_memory_blocks_final)

    def children(self) -> tuple[QPop[QPop.CompiledProps], ...]:
        return (self.input, )

    def pstr_more(self) -> Iterable[str]:
        yield ', '.join(expr.to_str() + ' ' + ('ASC' if asc else 'DESC')
                        for expr, asc in zip(self.exprs, self.orders_asc))
        yield f'# memory blocks: {self.num_memory_blocks} ({self.num_memory_blocks_final} last pass)'
        return

    def _infer_ordering_props(self) -> tuple[list[int], list[bool]]:
        ordered_columns: list[int] = list()
        ordered_asc: list[bool] = list()
        for expr, asc in zip(self.exprs, self.orders_asc):
            if (input_column_index := self.column_in_child(expr, 0)) is not None:
                ordered_columns.append(input_column_index)
                ordered_asc.append(asc)
            else: # sorting by something that's not an output column
                break # this gap would destroy the rest of the ordering
        if len(ordered_columns) == len(self.exprs): # no gap
            # our sort is stable, so previous ordering is still there, just pushed later:
            for input_column_index, asc in zip(self.input.compiled.ordered_columns, self.input.compiled.ordered_asc):
                if input_column_index not in ordered_columns:
                    ordered_columns.append(input_column_index)
                    ordered_asc.append(asc)
        return ordered_columns, ordered_asc

    def compare(self, this: tuple, that: tuple) -> int:
        """Compare two rows ``this`` and ``that``,
        and return ``-1``, ``0``, or ``1`` if ``this`` is less than (i.e., goes before in ascending order),
        equal to, or greater than ``that``, respectively.
        """
        return eval(self.compiled.cmp_exec, None, dict(this=this, that=that))

    def _compile_comparators(self) -> tuple[str, Any, Any, Any]:
        # construct the comparator for sorting:
        this_before_that_codes: list[str] = list()
        eq_codes: list[str] = list()
        for expr, asc in zip(self.exprs, self.orders_asc):
            op = '<' if asc else '>'
            this_code, _ = self.compile_valexpr(expr, ['this'])
            that_code, _ = self.compile_valexpr(expr, ['that'])
            this_before_that_code = f'{this_code}{op}{that_code}'
            if len(eq_codes) > 0:
                this_before_that_code = ' and '.join(eq_codes) + ' and ' + this_before_that_code
            this_before_that_codes.append(this_before_that_code)
            eq_codes.append(f'{this_code}=={that_code}')
        this_before_that_code = '(' + ') or ('.join(this_before_that_codes) + ')'
        eq_code = ' and '.join(eq_codes)
        cmp_code = f'-1 if {this_before_that_code} else (0 if ({eq_code}) else 1)'
        cmp_exec = compile(cmp_code, '<string>', 'eval')
        sort_key = cmp_to_key(self.compare) # for sort
        q_cmp = lambda this, that: ( # for priority queue
            t1 := cast(tuple, this),
            t2 := cast(tuple, that),
            cmp_result := self.compare(t1[0], t2[0]), # compare values
            cmp_result if cmp_result != 0 \
                else ((t1[-1] > t2[-1]) - (t2[-1] > t1[-1])) # comparing the run # ensures stable sort order
        )[-1]
        return cmp_code, cmp_exec, sort_key, q_cmp

    @cached_property
    def compiled(self) -> 'MergeSortPop.CompiledProps':
        input_props = self.input.compiled
        ordered_columns, ordered_asc = self._infer_ordering_props()
        cmp_code, cmp_exec, sort_key, q_cmp = self._compile_comparators()
        return MergeSortPop.CompiledProps.from_input(input_props,
                                                     ordered_columns = ordered_columns,
                                                     ordered_asc = ordered_asc,
                                                     cmp_code = cmp_code,
                                                     cmp_exec = cmp_exec,
                                                     sort_key = sort_key,
                                                     q_cmp = q_cmp)

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        stats = self.context.zm.selection_stats(self.input.estimated.stats, None)
        num_passes = 1
        num_runs = ceil(stats.block_count() / self.num_memory_blocks)
        while num_runs > self.num_memory_blocks_final:
            num_passes += 1
            num_runs = ceil(num_runs / (self.num_memory_blocks - 1))
        block_self_reads = stats.block_count() * (num_passes-1)
        block_self_writes = stats.block_count() * (num_passes-1)
        return QPop.EstimatedProps(
            stats = stats,
            blocks = QPop.EstimatedProps.StatsInBlocks(
                self_reads = block_self_reads,
                self_writes = block_self_writes,
                overall = self.input.estimated.blocks.overall + block_self_reads + block_self_writes))

    def _tmp_run_file(self, level: int, run: int) -> HeapFile:
        """Create a temporary file for a result run in a given level with an ordinal run number.
        Levels start at ``0`` (results of initial sorting pass) and go up by one with each additional merge pass.
        Each level may contain multiple result runs, numbered from ``0``.
        The file name is chosen in a way to help deduce which ``Pop`` produced it and what level and run number it has.
        """
        f = self.context.sm.heap_file(self.context.tmp_tx, f'.tmp-{hex(id(self))}-{level}-{run}', [], create_if_not_exists=True)
        f.truncate()
        return f

    def iter_merge(self, runs: list[HeapFile]) -> Generator[tuple, None, None]:
        """Merge the given sorted runs and return a stream of rows in the form of a generator.
        By the nature of :meth:`.HeapFile.iter_scan`, we essentially need one memory block for each run.
        """
        # construct a priority queue, where each entry is a triple (row, generator_where_it_came_from, run_#_where_it_came_from);
        # run_#_where_it_came_from is useful for ensuring a stable sort order.
        q: PQueue = PQueue(self.compiled.q_cmp)
        # initialize the queue with one row from each run:
        generators = [ run.iter_scan() for run in runs ]
        for i, generator in enumerate(generators):
            row = next(generator, None)
            if row is not None:
                q.enqueue((row, generator, i))
        # repeatedly dequeue rows to return, and
        # for each dequeued row, fetch the next from the same generator:
        while not q.empty():
            # grab the smallest row and output it:
            row, generator, i = cast(tuple[tuple, Generator[tuple, None, None], int], q.dequeue())
            yield row
            # enter the next row from the same generator (if any):
            row = next(generator, None)
            if row is not None:
                q.enqueue((row, generator, i))
        for generator in generators:
            generator.close()
        return

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        # initial sorting pass:
        # TODO: if the input fits in memory, there is actually no need to write it out;
        # the following code curently doesn't implement this optimization.
        logging.debug('***** pass 0: sort')
        runs: list[HeapFile] = list()
        for buffer in BufferedReader(self.num_memory_blocks).iter_buffer(self.input.execute()):
            buffer.sort(key=self.compiled.sort_key)
            run = self._tmp_run_file(0, len(runs))
            runs.append(run)
            run.batch_append(buffer) # write all sorted contents to the run
        # subsequent merge passes, up to the very last:
        level = 1
        while len(runs) > self.num_memory_blocks_final:
            logging.debug(f'***** pass {level}: merge {len(runs)} runs')
            new_runs: list[HeapFile] = list()
            for i in range(ceil(float(len(runs))/(self.num_memory_blocks-1))):
                # merge up to (self.num_memory_blocks-1) runs at a time:
                runs_subset = runs[i * (self.num_memory_blocks-1) : (i+1) * (self.num_memory_blocks-1)]
                new_run = self._tmp_run_file(level, len(new_runs))
                new_runs.append(new_run)
                writer = BufferedWriter(new_run, 1) # one block to buffer output
                for row in self.iter_merge(runs_subset):
                    writer.write(row)
                writer.flush() # make sure all buffered rows are written
                # delete the old runs:
                for run in runs_subset:
                    self.context.sm.delete_heap_file(self.context.tmp_tx, run.name)
            runs = new_runs
            level += 1
        # last pass to (merge and) stream results:
        logging.debug(f'***** pass {level}: final merging of {len(runs)} runs')
        for row in self.iter_merge(runs):
            yield row
        # delete the old runs:
        for run in runs:
            self.context.sm.delete_heap_file(self.context.tmp_tx, run.name)
        return
    