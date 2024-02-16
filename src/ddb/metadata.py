from typing import Final, Iterable, Generator
from collections import OrderedDict
from dataclasses import dataclass
import pickle

from .types import ValType, RowType
from .storage import StorageManager, HeapFile, BplusTree
from .transaction import Transaction

INTERNAL_TABLES_FILE_NAME: Final[str] = '.ddb_tables'
INTERNAL_ROW_ID_COLUMN_NAME: Final[str] = '.row_id'
INTERNAL_ROW_ID_COLUMN_TYPE: ValType = ValType.INTEGER
INTERNAL_ANON_TABLE_NAME_FORMAT: Final[str] = '.table_{pop}_{hex}'
INTERNAL_ANON_COLUMN_NAME_FORMAT: Final[str] = '.column_{index}'
INTERNAL_SECONDARY_INDEX_FILE_NAME_FORMAT: Final[str] = '.{table_name}.{column_name}'

@dataclass(frozen=True)
class TableMetadata:
    column_names: list[str]
    column_types: RowType

    def columns_as_ordered_dict(self) -> OrderedDict[str, str]:
        """Return an ordered dictionary where keys are column names and values are their types.
        The ordering is consistent with the ordering of columns in the schema.
        """
        return OrderedDict((n, t.name) for n, t in zip(self.column_names, self.column_types))

    def pstr(self) -> Iterable[str]:
        """Produce a sequence of lines for pretty-printing the object.
        """
        yield '(' + ', '.join(n + ' ' + t.name for n, t in zip(self.column_names, self.column_types)) + ')'
        return

@dataclass(frozen=True)
class BaseTableMetadata(TableMetadata):
    name: str
    """Name of the base table.
    """
    primary_key_column_index: int | None
    """Column index (into ``column_names`` and ``column_types``) for the (single-column) primary key,
    or ``None`` if this table has no primary key.
    """
    secondary_column_indices: list[int]
    """Column Indices (into ``column_names`` and ``column_types``) for all (single-column) secondary indexes on this table,
    in no particular order.
    """

    def id_name(self) -> str:
        if self.primary_key_column_index is None:
            return INTERNAL_ROW_ID_COLUMN_NAME
        else:
            return self.column_names[self.primary_key_column_index]

    def id_type(self) -> ValType:
        if self.primary_key_column_index is None:
            return INTERNAL_ROW_ID_COLUMN_TYPE
        else:
            return self.column_types[self.primary_key_column_index]

    def pstr(self) -> Iterable[str]:
        yield f'{self.name}(' +\
            ', '.join(n +\
                      ('[pk]' if i == self.primary_key_column_index else '') +\
                      ('[sk]' if i in self.secondary_column_indices else '') +\
                      ' ' + t.name
                      for i, (n, t) in enumerate(zip(self.column_names, self.column_types))) +\
            ')'
        return

class MetadataManager:
    """The metadata manager, which manages schema and
    also gives us heap file and B+tree handles by table names and index column names.
    """
    def __init__(self, sm: StorageManager) -> None:
        self.sm: Final = sm
        return

    def tables_btree(self, tx: Transaction) -> BplusTree:
        """Return (and create as needed) the internal B+tree for
        storing all schema information for the database.
        """
        return self.sm.bplus_tree(tx, INTERNAL_TABLES_FILE_NAME, ValType.VARCHAR, [ValType.ANY],
                                  unique = True, create_if_not_exists = True)

    def upsert_base_table_metadata(self, tx: Transaction, metadata: BaseTableMetadata) -> None:
        """Update (or create) metadata for the given table (``metadata.name``) in the schema.
        """
        with self.tables_btree(tx) as f:
            f.put(metadata.name, (pickle.dumps(metadata), ))
        return

    def delete_base_table_metadata(self, tx: Transaction, metadata: BaseTableMetadata) -> None:
        """Delete metadata for the given table (``metadata.name``) from the schema.
        """
        with self.tables_btree(tx) as f:
            f.delete(metadata.name)
        return

    def get_base_table_metadata(self, tx: Transaction, name: str) -> BaseTableMetadata | None:
        """Return metadata for the named table from the schema, or ``None`` if the table doesn't exist.
        """
        with self.tables_btree(tx) as f:
            payload = f.get_one(name)
            if payload is not None:
                return pickle.loads(payload[0])
            else:
                return None

    def list_base_tables(self, tx: Transaction) -> Generator[BaseTableMetadata, None, None]:
        """Return a Python generator that yields metadata for each table in the schema, one at a time.
        """
        with self.tables_btree(tx) as f:
            for name, payload in f.iter_scan():
                yield pickle.loads(payload[0])
        return

    def table_storage(self, tx: Transaction, metadata: BaseTableMetadata, create_if_not_exists: bool = False) -> HeapFile | BplusTree:
        """Return the storage (heap file or B+tree) object for the table with given ``metadata``
        (creating it as needed if requested by ``create_if_not_exists``).
        An exception will be raised if it is not found.
        """
        if metadata.primary_key_column_index is None:
            return self.sm.heap_file(tx, metadata.name, metadata.column_types, create_if_not_exists = create_if_not_exists)
        else:
            row_type = metadata.column_types.copy()
            key_type = row_type.pop(metadata.primary_key_column_index)
            return self.sm.bplus_tree(tx, metadata.name, key_type, row_type, unique = True, create_if_not_exists = create_if_not_exists)

    def remove_table_storage(self, tx: Transaction, metadata: BaseTableMetadata) -> None:
        """Remove the heap file or B+tree storage for the table with given ``metadata``.
        """
        if metadata.primary_key_column_index is None:
            self.sm.delete_heap_file(tx, metadata.name)
        else:
            self.sm.delete_bplus_tree(tx, metadata.name)
        return

    @staticmethod
    def _secondary_index_storage_name(table_name: str, column_name: str) -> str:
        return INTERNAL_SECONDARY_INDEX_FILE_NAME_FORMAT.format(table_name = table_name, column_name = column_name)

    def index_storage(self, tx: Transaction, metadata: BaseTableMetadata, column_index: int, create_if_not_exists: bool = False) -> BplusTree:
        """Return the B+tree object for the index on the given column for the table with given ``metadata``
        (creating it as needed if requested by ``create_if_not_exists``).
        An exception will be raised if it is not found.
        Note that the index can be either primary or secondary.
        """
        if column_index == metadata.primary_key_column_index:
            row_type = metadata.column_types.copy()
            key_type = row_type.pop(metadata.primary_key_column_index)
            return self.sm.bplus_tree(tx, metadata.name, key_type, row_type,
                                      unique = True, create_if_not_exists = create_if_not_exists)
        else:
            row_type = [INTERNAL_ROW_ID_COLUMN_TYPE]
            key_type = metadata.column_types[column_index]
            index_storage_name = type(self)._secondary_index_storage_name(metadata.name, metadata.column_names[column_index])
            return self.sm.bplus_tree(tx, index_storage_name, key_type, row_type,
                                      unique = False, create_if_not_exists = create_if_not_exists)

    def remove_secondary_index_storage(self, tx: Transaction, metadata: BaseTableMetadata, column_index: int) -> None:
        """Remove the B+tree object for the index on the given column for the table with given ``metadata``.
        """
        index_storage_name = type(self)._secondary_index_storage_name(metadata.name, metadata.column_names[column_index])
        self.sm.delete_bplus_tree(tx, index_storage_name)
        return
