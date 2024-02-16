from typing import Final, Iterable, Generator, Any
from dataclasses import dataclass
from functools import cached_property

from ..profile import profile_generator
from ..validator import ValExpr, valexpr
from ..validator.valexpr.eval import *

from .interface import QPop

class FilterPop(QPop['FilterPop.CompiledProps']):
    """Filter physical operator.  No extra memory is needed.
    """

    @dataclass
    class CompiledProps(QPop.CompiledProps):
        cond_code: str
        """Python code for filter condition.
        """
        cond_exec: Any
        """Python executable for filter condition.
        """

        def pstr(self) -> Iterable[str]:
            yield from super().pstr()
            if self.cond_code is not None:
                yield 'filter condition code: ' + self.cond_code
            return

    def __init__(self, input: QPop[QPop.CompiledProps], cond: ValExpr) -> None:
        """Construct a filter on top of the given ``input``.
        """
        super().__init__(input.context)
        self.input: Final = input
        self.cond: Final = cond
        return

    def memory_blocks_required(self) -> int:
        return 0

    def children(self) -> tuple[QPop[QPop.CompiledProps], ...]:
        return (self.input, )

    def pstr_more(self) -> Iterable[str]:
        yield 'filter condition: ' + self.cond.to_str()
        return

    @cached_property
    def compiled(self) -> 'FilterPop.CompiledProps':
        input_props = self.input.compiled
        code, exec = self.compile_valexpr(self.cond)
        return FilterPop.CompiledProps.from_input(input_props,
                                                  cond_code = code,
                                                  cond_exec = exec)

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        stats = self.context.zm.selection_stats(
            self.input.estimated.stats,
            valexpr.relativize(self.cond, [self.input.compiled.output_lineage]))
        return QPop.EstimatedProps(
            stats = stats,
            blocks = QPop.EstimatedProps.StatsInBlocks(
                self_reads = 0,
                self_writes = 0,
                overall = self.input.estimated.blocks.overall))

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        cond_exec = self.compiled.cond_exec
        for row in self.input.execute():
            if eval(cond_exec, None, dict(row0 = row)):
                yield row
        return
