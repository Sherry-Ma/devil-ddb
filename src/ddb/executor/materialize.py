from typing import Final, Iterable, Generator
from functools import cached_property

from ..profile import profile_generator
from ..storage import HeapFile

from .interface import QPop, ExecutorException
from .util import BufferedWriter

class MaterializePop(QPop[QPop.CompiledProps]):
    """Materialization physical operator.
    It caches all its input rows, using as many memory blocks as it is given and spilling to tmp space as needed,
    such that subsequent executions will simply return the cached rows without recomputing them.
    """

    def __init__(self, input: QPop[QPop.CompiledProps], blocking: bool = False, num_memory_blocks: int = 1) -> None:
        """Construct a materialization operator on top of the given ``input``,
        using the specified number of memory blocks (at least one) for caching/buffering.
        If ``blocking``, this operator will completely exhaust and materialize rows from its input operator,
        and then start returning.
        """
        super().__init__(input.context)
        self.input: Final = input
        self.blocking: Final = blocking
        self.num_memory_blocks: Final = num_memory_blocks
        if self.num_memory_blocks < 1:
            raise ExecutorException('materialization needs at least one memory block')
        self.writer: BufferedWriter | None = None
        return

    def memory_blocks_required(self) -> int:
        return self.num_memory_blocks

    def children(self) -> tuple[QPop[QPop.CompiledProps], ...]:
        return (self.input, )

    def pstr_more(self) -> Iterable[str]:
        yield f'# memory blocks: {self.num_memory_blocks}'
        return

    @cached_property
    def compiled(self) -> 'QPop.CompiledProps':
        input_props = self.input.compiled
        return QPop.CompiledProps.from_input(input_props)

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        stats = self.context.zm.selection_stats(self.input.estimated.stats, None)
        return QPop.EstimatedProps(
            stats = stats,
            blocks = QPop.StatsInBlocks(
                self_reads = stats.block_count(),
                self_writes = 0,
                overall = stats.block_count()),
            blocks_extra_init = QPop.StatsInBlocks(
                self_reads = 0,
                self_writes = stats.block_count(),
                overall = self.input.estimated.blocks.overall)) # note that subtree is paid as extra init cost

    def _tmp_file(self) -> HeapFile:
        """Create a temporary file for caching input rows.
        The file name is chosen in a way to help deduce which ``Pop`` produced it.
        """
        f = self.context.sm.heap_file(self.context.tmp_tx,
                                      f'.tmp-{hex(id(self))}-{hex(id(self.input))}',
                                      [], create_if_not_exists=True)
        f.truncate()
        return f

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        if self.writer is None:
            # first run, materialize!
            self.writer = BufferedWriter(self._tmp_file(), self.num_memory_blocks)
            for row in self.input.execute():
                self.writer.write(row)
                if not self.blocking:
                    yield row
            if self.writer.num_blocks_flushed > 0:
                # already spilled, so let's write buffered rows so the file is complete;
                # otherwise, no need to flush at all -- just use in-memory buffer:
                self.writer.flush()
            if not self.blocking: # already yielded results
                return
        # leverage the materialized results:
        if self.writer.num_blocks_flushed > 0: # leverage tmp file
            yield from self.writer.file.iter_scan()
        else: # leverage in-memory cache:
            for row in self.writer.buffer:
                yield row
        return

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if self.writer is not None:
            self.context.sm.delete_heap_file(self.context.tmp_tx, self.writer.file.name)
        return
