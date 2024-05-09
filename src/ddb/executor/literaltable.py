from typing import Final, Iterable, Generator
from functools import cached_property

from ..profile import profile_generator
from ..metadata import TableMetadata, INTERNAL_ANON_TABLE_NAME_FORMAT

from .interface import QPop, StatementContext

class LiteralTablePop(QPop[QPop.CompiledProps]):
    """A physical operator that produces the contents of a literal VALUES table.
    The rows are already stored in memory; no additional memory or I/O is needed.
    """

    def __init__(self, context: StatementContext, alias: str | None, metadata: TableMetadata, rows: list[tuple]) -> None:
        """We assume here that ``rows`` have exactly the same row type as specified by ``inferred_metadata``.
        """
        super().__init__(context)
        self.alias: Final = alias if alias is not None else\
            INTERNAL_ANON_TABLE_NAME_FORMAT.format(pop=type(self).__name__, hex=hex(id(self)))
        self.metadata: Final = metadata
        self.rows: Final = rows
        return

    def memory_blocks_required(self) -> int:
        return 0

    def children(self) -> tuple[QPop[QPop.CompiledProps], ...]:
        return tuple()

    def pstr_more(self) -> Iterable[str]:
        yield f'AS {self.alias}'
        yield from self.metadata.pstr()
        for row in self.rows:
            yield str(row)
        return

    @cached_property
    def compiled(self) -> QPop.CompiledProps:
        return QPop.CompiledProps(
            output_metadata = self.metadata,
            output_lineage = list(set(((self.alias, column_name), )) for column_name in self.metadata.column_names),
            ordered_columns = list(),
            ordered_asc = list(),
            unique_columns = set())

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        return QPop.EstimatedProps(
            stats = self.context.zm.literal_table_stats(self.rows),
            blocks = QPop.EstimatedProps.StatsInBlocks(
                self_reads = 0,
                self_writes = 0,
                overall = 0))

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        for row in self.rows:
            yield row
        return
