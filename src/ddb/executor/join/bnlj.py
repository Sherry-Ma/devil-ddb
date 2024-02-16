from typing import Final, Iterable, Generator, Any
from dataclasses import dataclass
from functools import cached_property
from math import ceil

from ...profile import profile_generator
from ...validator import ValExpr, valexpr
from ...validator.valexpr.eval import *

from ..interface import QPop
from ..util import BufferedReader

from .interface import JoinPop

class BNLJoinPop(JoinPop['BNLJoinPop.CompiledProps']):
    """Blocked-based nested-loop join physical operator.
    It will use as many memory blocks as it is given to buffer rows from the left (outer) input.
    The right (inner) input will simply be streamed in one row at a time.
    """

    @dataclass
    class CompiledProps(QPop.CompiledProps):
        cond_code: str | None
        """Python code for join condition.
        """
        cond_exec: Any
        """Python code for join condition.
        """

        def pstr(self) -> Iterable[str]:
            yield from super().pstr()
            if self.cond_code is not None:
                yield 'join condition code: ' + self.cond_code
            return

    def __init__(self, left: QPop[QPop.CompiledProps], right: QPop[QPop.CompiledProps], cond: ValExpr | None,
                 num_memory_blocks: int) -> None:
        """Construct a blocked-based nested-loop join between ``left`` and ``right`` inputs.
        ``cond`` is an optional join condition.
        It will use all memory blocks to buffer rows from the left (outer) input.
        """
        super().__init__(left, right)
        self.cond: Final = cond
        self.num_memory_blocks: Final = num_memory_blocks
        return

    def memory_blocks_required(self) -> int:
        return self.num_memory_blocks

    def pstr_more(self) -> Iterable[str]:
        if self.cond is not None:
            yield 'join condition: ' + self.cond.to_str()
        return

    @cached_property
    def compiled(self) -> 'BNLJoinPop.CompiledProps':
        code, exec = self.compile_valexpr(self.cond) if self.cond is not None else (None, None)
        return BNLJoinPop.CompiledProps.from_inputs(self.left.compiled, self.right.compiled,
                                                    cond_code = code,
                                                    cond_exec = exec)

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        stats = self.context.zm.join_stats(
            self.left.estimated.stats,
            self.right.estimated.stats,
            None if self.cond is None else\
                valexpr.relativize(self.cond, [self.left.compiled.output_lineage, self.right.compiled.output_lineage]))
        num_right_passes = ceil(self.left.estimated.stats.block_count() / self.num_memory_blocks)
        return QPop.EstimatedProps(
            stats = stats,
            blocks = QPop.EstimatedProps.StatsInBlocks(
                self_reads = 0,
                self_writes = 0,
                overall = self.left.estimated.blocks.overall +\
                    num_right_passes * self.right.estimated.blocks.overall))

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        outer = BufferedReader(self.num_memory_blocks)
        cond_exec = self.compiled.cond_exec
        for outer_buffer in outer.iter_buffer(self.left.execute()):
            for inner_row in self.right.execute():
                for outer_row in outer_buffer:
                    if cond_exec is None or eval(cond_exec, None, dict(row0 = outer_row, row1 = inner_row)):
                        yield (*outer_row, *inner_row)
        return
