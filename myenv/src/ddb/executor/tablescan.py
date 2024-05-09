from typing import Final, Iterable, Generator
from functools import cached_property

from ..metadata import INTERNAL_ROW_ID_COLUMN_NAME, INTERNAL_ROW_ID_COLUMN_TYPE
from ..profile import profile_generator
from ..storage import HeapFile, BplusTree
from ..validator import OutputLineage
from ..metadata import TableMetadata, BaseTableMetadata, ValType

from .interface import QPop, StatementContext

class TableScanPop(QPop[QPop.CompiledProps]):
    """Table scan physical operator.
    The operator calls the storage manager to perform the scan, which essentially uses one memory block to buffer input.
    For a table with no primary key, the underlying storage is a :class:`.HeapFile`;
    if row id is requested, this scan will return the row id as the first column, followed by the normal columns.
    For a table with a primary key, the underlying storage is :class:`.BplusTree`;
    this scan will return the primary key as the first column, followed by the rest of the columns.
    """
    def __init__(self, context: StatementContext, alias: str, meta: BaseTableMetadata, return_row_id: bool = False):
        """Construct a table scan for the database table whose metadata is given by ``meta``,
        with table alias ``alias``.
        ``return_row_id`` option only matters for a table with no primary key.
        """
        super().__init__(context)
        self.alias: Final = alias
        self.meta: Final = meta
        self.return_row_id: Final = return_row_id
        return

    def memory_blocks_required(self) -> int:
        return 1

    def children(self) -> tuple[QPop[QPop.CompiledProps], ...]:
        return tuple()

    def pstr_more(self) -> Iterable[str]:
        yield f'{self.meta.name} AS {self.alias}'

    @cached_property
    def compiled(self) -> QPop.CompiledProps:
        output_column_names: list[str] = list()
        output_column_types: list[ValType] = list()
        output_lineage: OutputLineage = list()
        ordered_columns: list[int] = list()
        ordered_asc: list[bool] = list()
        unique_columns: set[int] = set()
        for i, (column_name, column_type) in enumerate(zip(self.meta.column_names, self.meta.column_types)):
            # key is the first column that gets read out
            if self.meta.primary_key_column_index is not None and i == self.meta.primary_key_column_index:
                insert_i = 0
                ordered_columns = [0]
                ordered_asc = [True]
                unique_columns = {0}
            else:
                insert_i = len(output_column_names)
            output_column_names.insert(insert_i, column_name)
            output_column_types.insert(insert_i, column_type)
            output_lineage.insert(insert_i, set(((self.alias, column_name), )))
        if self.return_row_id and self.meta.primary_key_column_index is None:
            # prepend the row id column
            output_column_names.insert(0, INTERNAL_ROW_ID_COLUMN_NAME)
            output_column_types.insert(0, INTERNAL_ROW_ID_COLUMN_TYPE)
            output_lineage.insert(0, set(((self.alias, INTERNAL_ROW_ID_COLUMN_NAME), )))
            ordered_columns = [0]
            ordered_asc = [True]
            unique_columns = {0}
        return QPop.CompiledProps(\
            output_metadata = TableMetadata(output_column_names, output_column_types),
            output_lineage = output_lineage,
            ordered_columns = ordered_columns,
            ordered_asc = ordered_asc,
            unique_columns = unique_columns)

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        stats = self.context.zm.base_table_stats(self.context, self.meta, return_row_id=self.return_row_id)
        block_self_reads = stats.block_count()
        if stats.tree_height is not None:
            block_self_reads += max(stats.tree_height - 1, 0)
        block_self_writes = 0
        block_overall = block_self_reads + block_self_writes
        return QPop.EstimatedProps(
            stats = stats,
            blocks = QPop.EstimatedProps.StatsInBlocks(
                self_reads = block_self_reads,
                self_writes = block_self_writes,
                overall = block_overall))

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        with self.context.mm.table_storage(self.context.tx, self.meta) as file:
            if isinstance(file, HeapFile):
                for row in file.iter_scan(return_row_id=self.return_row_id):
                    yield row
            elif isinstance(file, BplusTree):
                for key, row in file.iter_scan():
                    yield (key, *row) # key is the first column that gets read out
        return
