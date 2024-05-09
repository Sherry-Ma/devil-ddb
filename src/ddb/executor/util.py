"""Various utility classes for query execution.
"""
from typing import cast, Final, Iterator, Generator, Callable, Any
from sys import getsizeof
from math import ceil
from functools import cmp_to_key, total_ordering
from queue import PriorityQueue
from sortedcontainers import SortedSet # type: ignore
import logging

from ..globals import BLOCK_SIZE
from ..storage import HeapFile

from .interface import ExecutorException

class BufferedReader:
    """Read and buffer rows from an input iterator in memory and serve them a chunk at a time,
    such that contents in the current chunk can be accessed without asking the input iterator again.
    """

    def __init__(self, num_memory_blocks: int) -> None:
        """Construct a buffered reader using the specified number of memory blocks.
        """
        self.num_memory_blocks: Final = num_memory_blocks
        self.max_bytes: Final[int] = num_memory_blocks * BLOCK_SIZE
        return

    def iter_buffer(self, input: Iterator[tuple]) -> Iterator[list[tuple]]:
        """Return an iterator that provides a buffer (list) of input rows at a time.
        Until ``next()``, the current list of rows is guaranteed to remain accessible in memory.
        """
        buffer: list[tuple] = list()
        num_bytes = 0
        for row in input:
            row_size = getsizeof(row) # perhaps not very precise, but oh well
            if row_size > self.max_bytes:
                raise ExecutorException(f'row too big to fix in {self.num_memory_blocks} block(s): {row}')
            if num_bytes + row_size > self.max_bytes:
                yield buffer # a full buffer is ready for consumption
                # clear the buffer: ready for next()
                buffer = list()
                num_bytes = 0
            buffer.append(row)
            num_bytes += row_size
        # make sure any remaining input rows are returned:
        if len(buffer) > 0:
            yield buffer
        return

class BufferedWriter:
    """Buffer rows to be appended to a :class:`.HeapFile`,
    such that all contents in the buffer can be written in one go.
    Flushing is only as needed or requested: in other words,
    if there is enough memory to buffer all rows, the file may not be touched at all.
    """

    def __init__(self, file: HeapFile, num_memory_blocks: int) -> None:
        """Construct a buffered writer using the specified number of memory blocks.
        The given file should already be opened within the appropriate transaction context,
        and this writer is not responsible for closing it.
        """
        self.file: Final[HeapFile] = file
        self.num_memory_blocks: Final = num_memory_blocks
        self.max_bytes: Final[int] = num_memory_blocks * BLOCK_SIZE
        self.buffer: Final[list[tuple]] = list()
        self.num_bytes = 0
        self.num_blocks_flushed = 0
        return

    def write(self, row: tuple) -> None:
        """Write a row, and automatically flush if we run out of buffer space.
        """
        row_size = getsizeof(row) # perhaps not very precise, but oh well
        self.buffer.append(row)
        self.num_bytes += row_size
        if self.num_bytes + row_size > self.max_bytes:
            self.flush()
        return

    def flush(self) -> None:
        """Flush the buffer.
        """
        self.file.batch_append(self.buffer)
        self.num_blocks_flushed += 1
        self.buffer.clear()
        self.num_bytes = 0
        return

class PQueue(PriorityQueue):
    """A priority queue with a custom comparator function.
    """

    @total_ordering
    class _WrappedItem:
        """Internal helper class used to remember the custom comparator.
        """
        def __init__(self, item: Any, cmp: Callable[[Any, Any], int]) -> None:
            self.item: Final = item
            self.cmp: Final = cmp

        def __eq__(self, other: object) -> bool:
            return self.cmp(self.item, cast('PQueue._WrappedItem', other).item) == 0

        def __lt__(self, other: object) -> bool:
            return self.cmp(self.item, cast('PQueue._WrappedItem', other).item) < 0

    def __init__(self, cmp: Callable[[Any, Any], int]) -> None:
        self.cmp: Final = cmp
        super().__init__()
        return

    def enqueue(self, item: Any) -> None:
        """Add an item to the queue.
        """
        super().put(PQueue._WrappedItem(item, self.cmp))
        return

    def dequeue(self) -> Any:
        """Remove the smallest item from the queue.
        """
        return super().get().item

class ExtSortBuffer:
    """Buffer rows, sort them (with optional deduplication), and allow them to be iterated over in order.
    If there is enough memory to buffer all rows, no I/Os will be involved.
    Otherwise, an external-memory merge sort is performed.
    """
    def __init__(self,
                 compare: Callable[[tuple, tuple], int],
                 tmp_file_create: Callable[[int, int], HeapFile],
                 tmp_file_delete: Callable[[HeapFile], None],
                 num_memory_blocks: int, num_memory_blocks_final: int | None = None,
                 deduplicate: bool = False) -> None:
        """Construct a sorting buffer using the specified number of memory blocks.
        You can add rows to this buffer and then get them back in sorted order (optionally deduplicated).
        Beware, however, that you must finish adding all rows before retrieving any of them.
        ``compare(this, that)`` should allow this writer to compare two rows``this`` and ``that``,
        and return ``-1``, ``0``, or ``1`` if ``this`` is less than (i.e., goes before in ascending order),
        equal to, or greater than ``that``, respectively.
        This buffer will use temporary files as needed when it runs out of memory.
        ``tmp_file_make(level, run)`` should allow this writer to create temporary files within the appropriate transaction context:
        ``level`` starts at ``0`` (results of initial sorting pass) and go up by one with each additional merge pass;
        each pass can produce multiple result runs, numbered from ``0``.
        ``tmp_file_delete(heapfile)`` should allow this writer to delete a temporary file.
        If ``deduplicate`` is ``True``, duplicates will be removed;
        here, duplicates are defined as rows that satisfy ``==`` (a condition stronger than ``compare`` returning ``0``).
        """
        self.compare: Final = compare
        self.sort_key: Final = cmp_to_key(self.compare)
        self.tmp_file_create: Final = tmp_file_create
        self.tmp_file_delete: Final = tmp_file_delete
        self.num_memory_blocks: Final = num_memory_blocks
        if self.num_memory_blocks <= 2:
            raise ExecutorException('merge sort needs at least 3 memory blocks to perform a merge')
        self.num_memory_blocks_final: Final = num_memory_blocks_final or self.num_memory_blocks
        if self.num_memory_blocks_final <= 1:
            raise ExecutorException('merge sort needs at least 2 memory blocks to perform the final merge')
        self.max_bytes: Final[int] = num_memory_blocks * BLOCK_SIZE
        self.deduplicate: Final = deduplicate
        self.buffer: list|SortedSet =\
            SortedSet(key=self.sort_key) if self.deduplicate else\
            list()
        self.num_bytes: int = 0
        self.num_blocks_flushed: int = 0
        self.runs: list[HeapFile] = list()
        return

    def _flush(self) -> None:
        """Flush the in-memory buffer and empty it.
        """
        run: HeapFile = self.tmp_file_create(0, len(self.runs))
        self.runs.append(run)
        if self.deduplicate:
            run.batch_append(self.buffer)
            self.buffer = SortedSet(key=self.sort_key)
        else:
            self.buffer.sort(key=self.sort_key)
            run.batch_append(self.buffer)
            self.buffer = list()
        self.num_bytes = 0
        self.num_blocks_flushed += self.num_memory_blocks
        return

    def add(self, row: tuple) -> None:
        """Add a row, and automatically spill to temporary file if we run out of buffer space.
        """
        if self.deduplicate and row in self.buffer:
            return
        row_size = getsizeof(row) # perhaps not very precise, but oh well
        if self.num_bytes + row_size > self.max_bytes: # flush rows in memory first
            self._flush()
        # add the new row:
        if isinstance(self.buffer, SortedSet):
            self.buffer.add(row)
        else:
            self.buffer.append(row)
        self.num_bytes += row_size
        return

    def _iter_merge(self, runs: list[HeapFile]) -> Generator[tuple, None, None]:
        """Merge the given sorted runs and return a stream of rows in the form of a generator.
        By the nature of :meth:`.HeapFile.iter_scan`, we essentially need one memory block for each run.
        """
        # construct a priority queue, where each entry is a triple (row, generator_where_it_came_from, run_#_where_it_came_from);
        # run_#_where_it_came_from is useful for ensuring a stable sort order.
        q_cmp = lambda this, that: (
            t1 := cast(tuple, this),
            t2 := cast(tuple, that),
            cmp_result := self.compare(t1[0], t2[0]), # compare values
            cmp_result if cmp_result != 0 \
                else ((t1[-1] > t2[-1]) - (t2[-1] > t1[-1])) # comparing the run # ensures stable sort order
        )[-1]
        q: PQueue = PQueue(q_cmp)
        # initialize the queue with one row from each run:
        generators = [ run.iter_scan() for run in runs ]
        for i, generator in enumerate(generators):
            row = next(generator, None)
            if row is not None:
                q.enqueue((row, generator, i))
        # repeatedly dequeue rows to return, and
        # for each dequeued row, fetch the next from the same generator:
        last_dequeued: tuple|None = None
        while not q.empty():
            # grab the smallest row and output it:
            row, generator, i = cast(tuple[tuple, Generator[tuple, None, None], int], q.dequeue())
            if not self.deduplicate or last_dequeued != row:
                yield row
            last_dequeued = row
            # enter the next row from the same generator (if any):
            row = next(generator, None)
            if row is not None:
                q.enqueue((row, generator, i))
        for generator in generators:
            generator.close()
        return

    def iter_and_clear(self) -> Generator[tuple, None, None]:
        """Return a Python generator that iterates over the added rows in sorted order.
        When the iteration completes, all rows will be cleared, and the buffer will be ready to accept new rows.
        """
        # check if we can just do this completely in memory;
        # if not, we have to flush what's in memory as a run and then start merging:
        if self.num_blocks_flushed == 0:
            if self.deduplicate:
                yield from self.buffer
                self.buffer = SortedSet(key=self.sort_key)
                return
            else:
                self.buffer.sort(key=self.sort_key)
                yield from self.buffer
                self.buffer = list()
                return
        elif len(self.buffer) > 0:
            self._flush()
        # subsequent merge passes, up to the very last:
        level = 1
        while len(self.runs) > self.num_memory_blocks_final:
            logging.debug(f'***** pass {level}: merge {len(self.runs)} runs')
            new_runs: list[HeapFile] = list()
            for i in range(ceil(float(len(self.runs))/(self.num_memory_blocks-1))):
                # merge up to (self.num_memory_blocks-1) runs at a time:
                runs_subset = self.runs[i * (self.num_memory_blocks-1) : (i+1) * (self.num_memory_blocks-1)]
                new_run = self.tmp_file_create(level, len(new_runs))
                new_runs.append(new_run)
                writer = BufferedWriter(new_run, 1) # one block to buffer output
                for row in self._iter_merge(runs_subset):
                    writer.write(row)
                writer.flush() # make sure all buffered rows are written
                # delete the old runs:
                for run in runs_subset:
                    self.tmp_file_delete(run)
            self.runs = new_runs
            level += 1
        # last pass to (merge and) stream results:
        logging.debug(f'***** pass {level}: final merging of {len(self.runs)} runs')
        for row in self._iter_merge(self.runs):
            yield row
        # finally, delete the runs:
        for run in self.runs:
            self.tmp_file_delete(run)
        self.runs = list()
        return
