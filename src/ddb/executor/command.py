from typing import cast, Iterable
from contextlib import ExitStack
import logging

from ..globals import DEFAULT_SORT_BUFFER_SIZE
from ..metadata import BaseTableMetadata, INTERNAL_ROW_ID_COLUMN_NAME, INTERNAL_ROW_ID_COLUMN_TYPE
from ..validator import valexpr
from ..storage import HeapFile

from .interface import ExecutorException, CPop, QPop, StatementContext
from .tablescan import TableScanPop
from .project import ProjectPop
from .mergesort import MergeSortPop

class CreateTablePop(CPop):
    def __init__(self, context: StatementContext, metadata: BaseTableMetadata) -> None:
        super().__init__(context)
        self.metadata = metadata
        return

    def execute(self) -> str:
        self.context.mm.table_storage(self.context.tx, self.metadata, create_if_not_exists=True)
        self.context.mm.upsert_base_table_metadata(self.context.tx, self.metadata)
        return 'CREATE TABLE'

class AnalyzeStatsPop(CPop):
    def __init__(self, context: StatementContext, base_metas: list[BaseTableMetadata] | None) -> None:
        super().__init__(context)
        self.base_metas = base_metas
        return

    def execute(self) -> str:
        for s in self.context.zm.analyze_stats(self.context, self.base_metas).pstr():
            logging.info(s)
        return 'ANALYZE' + ((' ' + ', '.join(meta.name for meta in self.base_metas)) if self.base_metas is not None else '')

class ShowTablesPop(CPop):
    def __init__(self, context: StatementContext) -> None:
        super().__init__(context)
        return

    def execute(self) -> str:
        lines = list()
        count = 0
        for table_metadata in self.context.mm.list_base_tables(self.context.tx):
            for s in table_metadata.pstr():
                lines.append(s)
            count += 1
        lines.append(f'SELECT {count}')
        return '\n'.join(lines)

class CreateIndexPop(CPop):
    def __init__(self, context: StatementContext, metadata: BaseTableMetadata, column_index: int) -> None:
        super().__init__(context)
        self.metadata = metadata
        self.column_index = column_index
        return

    def execute(self) -> str:
        self.metadata.secondary_column_indices.append(self.column_index)
        self.context.mm.upsert_base_table_metadata(self.context.tx, self.metadata)
        if self.metadata.primary_key_column_index is None:
            row_id_ref = valexpr.leaf.NamedColumnRef(
                self.metadata.name,
                INTERNAL_ROW_ID_COLUMN_NAME,
                INTERNAL_ROW_ID_COLUMN_TYPE)
        else:
            row_id_ref = valexpr.leaf.NamedColumnRef(
                self.metadata.name,
                self.metadata.column_names[self.metadata.primary_key_column_index],
                self.metadata.column_types[self.metadata.primary_key_column_index])
        index_col_ref = valexpr.leaf.NamedColumnRef(
            self.metadata.name,
            self.metadata.column_names[self.column_index],
            self.metadata.column_types[self.column_index])
        scan = MergeSortPop(ProjectPop(TableScanPop(self.context, self.metadata.name, self.metadata,
                                                    return_row_id=(self.metadata.primary_key_column_index is None)),
                                       [row_id_ref, index_col_ref], None),
                            [index_col_ref, row_id_ref], [True, True],
                            DEFAULT_SORT_BUFFER_SIZE, DEFAULT_SORT_BUFFER_SIZE)
        count = 0
        with self.context.mm.index_storage(self.context.tx, self.metadata, self.column_index, create_if_not_exists=True) as f:
            for val, row_id in scan.execute():
                f.put(val, (row_id, ))
                count += 1
        return f'CREATE INDEX {count}'

class InsertPop(CPop):
    def __init__(self, context: StatementContext, metadata: BaseTableMetadata, contents_query: QPop) -> None:
        """We assume here that ``contents_query`` produces rows that are of the exact same type as specified by ``metadata``,
        i.e., no conversion is needed.
        """
        super().__init__(context)
        self.metadata = metadata
        self.contents_query = contents_query
        return

    def pstr_more(self) -> Iterable[str]:
        yield from self.contents_query.pstr()
        return

    def execute(self) -> str:
        count = 0
        with self.context.mm.table_storage(self.context.tx, self.metadata) as f, ExitStack() as stack:
            secondary_indices = [stack.enter_context(self.context.mm.index_storage(self.context.tx, self.metadata, i)) \
                                 for i in self.metadata.secondary_column_indices]
            if isinstance(f, HeapFile):
                for row in self.contents_query.execute():
                    row_id = f.put(row)
                    for i, si in zip(self.metadata.secondary_column_indices, secondary_indices):
                        si.put(row[i], (row_id, ))
                    count += 1
            else:
                for row in self.contents_query.execute():
                    key = row[cast(int, self.metadata.primary_key_column_index)]
                    if f.get_one(key) is not None:
                        raise ExecutorException(f'primary key constraint violation in {self.metadata.name}: key value {key}')
                    rest_of_row = tuple(v for i, v in enumerate(row) if i != self.metadata.primary_key_column_index)
                    f.put(key, rest_of_row)
                    for i, si in zip(self.metadata.secondary_column_indices, secondary_indices):
                        si.put(row[i], (key, ))
                    count += 1
        return f'INSERT {count}'

class DeletePop(CPop):
    def __init__(self, context: StatementContext, metadata: BaseTableMetadata, key_query: QPop) -> None:
        super().__init__(context)
        self.metadata = metadata
        self.key_query = key_query
        return

    def pstr_more(self) -> Iterable[str]:
        yield from self.key_query.pstr()
        return

    def execute(self) -> str:
        count = 0
        with self.context.mm.table_storage(self.context.tx, self.metadata) as f, ExitStack() as stack:
            secondary_indices = [stack.enter_context(self.context.mm.index_storage(self.context.tx, self.metadata, i)) \
                                 for i in self.metadata.secondary_column_indices]
            for row in self.key_query.execute():
                f.delete(row[0])
                for i, si in enumerate(secondary_indices):
                    key = row[i+1] # offset is 1 because row[0] is the id
                    si.delete(key, (row[0],))
                count += 1
        return f'DELETE {count}'
