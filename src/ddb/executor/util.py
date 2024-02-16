"""Various utility classes for query execution.
"""
from typing import cast, Final, Iterator, Callable, Any
from sys import getsizeof
from functools import total_ordering
from queue import PriorityQueue

from ..globals import BLOCK_SIZE
from ..storage import HeapFile

from .interface import ExecutorException, StatementContext

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
