"""This module contains the implementation of a storage manager
based on `LMDB <https://lmdb.readthedocs.io/en/release/>`_.
"""
from typing import Final, Generator, Iterable, Callable, Any
from abc import abstractmethod
from math import ceil

import lmdb # type: ignore

from .. import globals
from ..profile import profile, profile_generator, ProfileStat
from ..types import ValType, RowType
from ..transaction import Transaction, TransactionManager

from .interface import StorageMangerException, HeapFile, BplusTree, StorageManager
from .serialize import pack_int, unpack_int, pack_str, unpack_str, pack_row, unpack_row

class LMDBTransactionInterface(Transaction):
    """Defines the minimally required interface for a transaction object expected by :class:`LMDBStorageManager`.
    Specifically, it must have a LMDB transaction handle.
    """
    @abstractmethod
    def __init__(self, tm: TransactionManager, id: int, lmdb_tx: lmdb.Transaction, read_only: bool = False) -> None:
        super().__init__(tm, id, read_only = read_only)
        self.lmdb_tx: Final[lmdb.Transaction] = lmdb_tx
        return

class LMDBHeapFile(HeapFile):
    """LMDB-based heap file implementation.
    """

    class MyProfileStat(ProfileStat):
        """Customized profile collector for some :class:`LMDBHeapFile` methods.
        See also :mod:`.profile`.

        Note: counting block I/O is tricky because LMDB does not expose that to us.
        Instead, we make our best guestimate based on the number of rows accessed.
        Hence, the values of ``num_blocks_read`` and ``num_blocks_written`` are sensible only after :meth:`.finalize()`,
        and not during the iteration.
        """
        def __init__(self, method: Callable, obj: 'LMDBHeapFile', caller: ProfileStat | None,
                     *call_args, **call_kw):
            super().__init__(method, obj, caller, *call_args, **call_kw)
            self.obj: Final = obj
            self._method_name: Final = method.__name__
            if self._method_name not in ('get', 'iter_scan', 'put', 'batch_append', 'delete'):
                raise NotImplementedError(f'I/O stats for {method.__qualname__} not available')
            self._stat = obj.stat()
            # account for the initial lookup:
            self.num_blocks_read += self._stat['depth']
            return

        def _estimate_blocks(self, num_entries: int) -> int:
            if self._stat['leaf_pages'] == 0 or self._stat['entries'] == 0:
                self._stat = self.obj.stat() # force refresh stats
            stat = self._stat
            # use total number of leaves and entries to come up with a guestimate:
            # WARNING: we cannot set LMDB block size, but we can "pretend" leaves use our own block size:
            leaves = ceil(stat['leaf_pages'] * stat['psize'] / globals.BLOCK_SIZE)
            if leaves == 0:
                return 0
            entries_per_block = ceil(float(stat['entries']) / leaves)
            if entries_per_block == 0: # no entries
                return 0
            return ceil(float(num_entries) / entries_per_block)

        def finalize(self, result: Any) -> None:
            super().finalize(result)
            if self._method_name in ('iter_scan'):
                self.num_blocks_read += self._estimate_blocks(self.num_next_calls)
                if self.num_blocks_read > 1:
                    self.num_blocks_read -= 1 # because we included the first leaf in the initial lookup cost
            elif self._method_name == 'put':
                self.num_blocks_written = 1
            elif self._method_name == 'delete' and result > 0: # result is number of records deleted
                self.num_blocks_written = 1
            elif self._method_name == 'batch_append': # result[1] is number of records appended
                self.num_blocks_written = self._estimate_blocks(result[1])
            return

    def __init__(self, storage_manager: 'LMDBStorageManager', tx: LMDBTransactionInterface, name: str, row_type: RowType) -> None:
        super().__init__(tx, name, row_type)
        self.storage_manager: Final = storage_manager
        self.lmdb_tx: Final = tx.lmdb_tx
        self.lmdb_handle = None
        return

    def _open(self, create_if_not_exists: bool = False) -> None:
        if self.lmdb_handle is None:
            file_key = pack_str(f'${self.__class__.__qualname__}.{self.name}')
            self.lmdb_handle = (self.storage_manager.tmp_env if self.tx.is_tmp() else self.storage_manager.env)\
                .open_db(key=file_key, create=create_if_not_exists, txn=self.lmdb_tx)
        return

    def stat(self) -> dict:
        return self.lmdb_tx.stat(self.lmdb_handle)

    @profile(MyProfileStat)
    def get(self, row_id: int) -> tuple | None:
        bytes = self.lmdb_tx.get(pack_int(row_id), db=self.lmdb_handle)
        return unpack_row(bytes) if bytes is not None else None

    @profile_generator(MyProfileStat)
    def iter_scan(self, return_row_id: bool = False) -> Generator[tuple, None, None]:
        with self.lmdb_tx.cursor(db=self.lmdb_handle) as cursor:
            for k, v in cursor:
                if return_row_id:
                    yield unpack_int(k), *(unpack_row(v))
                else:
                    yield unpack_row(v)
        return

    @profile(MyProfileStat)
    def put(self, row: tuple, row_id: int | None = None) -> int:
        if row_id is None:
            with self.lmdb_tx.cursor(db=self.lmdb_handle) as cursor:
                if cursor.last():
                    row_id = unpack_int(cursor.key()) + 1
                else: # file is empty
                    row_id = 0
        self.lmdb_tx.put(pack_int(row_id), pack_row(row), db=self.lmdb_handle)
        return row_id

    @profile(MyProfileStat)
    def batch_append(self, rows: Iterable[tuple]) -> tuple[int, int]:
        with self.lmdb_tx.cursor(db=self.lmdb_handle) as cursor:
            if cursor.last():
                row_id_start = unpack_int(cursor.key()) + 1
            else: # file is empty
                row_id_start = 0
            row_id = row_id_start
            for row in rows:
                assert cursor.put(pack_int(row_id), pack_row(row), append=True)
                row_id += 1
            return row_id_start, row_id - row_id_start

    def truncate(self) -> int:
        num_entries: int = self.stat()['entries']
        if num_entries > 0:
            # quickest way to truncate is to drop and recreate:
            self.lmdb_tx.drop(self.lmdb_handle, delete=True)
            self._close()
            self._open(create_if_not_exists=True)
        return num_entries

    @profile(MyProfileStat)
    def delete(self, row_id: int) -> int:
        if self.lmdb_tx.delete(pack_int(row_id), db=self.lmdb_handle) > 0:
            return 1
        else:
            return 0

    def _close(self):
        """NOTE: lmdb's Python binding designers decided not to expose the ability
        to close lmdb file handles in API (underlying lmdb actually reuse handles
        across transactions, so there is a possibility of inadvertently
        closing handles while they are still used by others).
        However, leaving all handles around is known to cause performance issues.
        There isn't much we could do here for now, but we will keep an eye on it.
        """
        self.lmdb_handle = None
        return

class LMDBBplusTree(BplusTree):
    """LMDB-based B+tree implementation.
    """

    class MyProfileStat(ProfileStat):
        """Customized profile collector for some :class:`LMDBBplusTree` methods.
        See also :mod:`.profile`.

        Note: counting block I/O is tricky because LMDB does not expose that to us.
        Instead, we make our best guestimate based on the number of rows accessed.
        Hence, the values of ``num_blocks_read`` and ``num_blocks_written`` are sensible only after :meth:`.finalize()`,
        and not during the iteration.
        """
        def __init__(self, method: Callable, obj: 'LMDBBplusTree', caller: ProfileStat | None,
                     *call_args, **call_kw):
            super().__init__(method, obj, caller, *call_args, **call_kw)
            self.obj: Final = obj
            self._method_name: Final = method.__name__
            if self._method_name not in ('get_one', 'iter_get', 'iter_scan', 'put', 'delete'):
                raise NotImplementedError(f'I/O stats for {method.__qualname__} not available')
            self._stat = obj.stat()
            # account for the initial lookup:
            self.num_blocks_read += self._stat['depth']
            return

        def _estimate_blocks(self, num_entries: int) -> int:
            if self._stat['leaf_pages'] == 0 or self._stat['entries'] == 0:
                self._stat = self.obj.stat() # force refresh stats
            stat = self._stat
            # use total number of leaves and entries to come up with a guestimate:
            # WARNING: we cannot set LMDB block size, but we can "pretend" leaves use our own block size:
            leaves = ceil(stat['leaf_pages'] * stat['psize'] / globals.BLOCK_SIZE)
            if leaves == 0:
                return 0
            entries_per_block = ceil(float(stat['entries']) / leaves)
            if entries_per_block == 0: # no entries
                return 0
            return ceil(float(num_entries) / entries_per_block)

        def finalize(self, result: Any) -> None:
            super().finalize(result)
            # get_one case doesn't need any additional adjustment
            if self._method_name in ('iter_get', 'iter_scan'):
                self.num_blocks_read += self._estimate_blocks(self.num_next_calls)
                if self.num_blocks_read > 1:
                    self.num_blocks_read -= 1 # because we included the first leaf in the initial lookup cost
            elif self._method_name == 'put':
                self.num_blocks_written = 1
            elif self._method_name == 'delete' and result > 0: # result is number of records deleted
                self.num_blocks_written = self._estimate_blocks(result)
            return

    def __init__(self, storage_manager: 'LMDBStorageManager',
                 tx: LMDBTransactionInterface, name: str,
                 key_type: ValType, row_type: RowType,
                 unique: bool = False) -> None:
        super().__init__(tx, name, key_type, row_type, unique)
        self.storage_manager = storage_manager
        self.lmdb_tx: Final = tx.lmdb_tx
        self.lmdb_handle = None
        pack: Callable[[Any], bytes] | None = None
        unpack: Callable[[bytes], Any] | None = None
        if self.key_type == ValType.INTEGER:
            pack, unpack = pack_int, unpack_int
        elif self.key_type == ValType.VARCHAR:
            pack, unpack = pack_str, unpack_str
        else:
            raise StorageMangerException(f'{self.name}: B+tree does not support {self.key_type.name} key')
        self.pack_key: Final = pack
        self.unpack_key: Final = unpack
        return

    def _open(self, create_if_not_exists: bool = False) -> None:
        if self.lmdb_handle is None:
            file_key = pack_str(f'${self.__class__.__qualname__}.{self.name}')
            self.lmdb_handle = (self.storage_manager.tmp_env if self.tx.is_tmp() else self.storage_manager.env)\
                .open_db(key=file_key, dupsort=(not self.unique),
                         create=create_if_not_exists, txn=self.lmdb_tx)
        return

    def stat(self) -> dict:
        stats = self.lmdb_tx.stat(db=self.lmdb_handle)
        return stats

    @profile(MyProfileStat)
    def get_one(self, key: Any) -> tuple | None:
        bytes = self.lmdb_tx.get(self.pack_key(key), db=self.lmdb_handle)
        return unpack_row(bytes) if bytes is not None else None

    @profile_generator(MyProfileStat)
    def iter_get(self, key: Any) -> Generator[tuple, None, None]:
        if self.unique:
            v = self.lmdb_tx.get(self.pack_key(key), db=self.lmdb_handle)
            if v is not None:
                yield key, unpack_row(v)
        else:
            with self.lmdb_tx.cursor(db=self.lmdb_handle) as cursor:
                cursor.set_key(self.pack_key(key))
                for k, v in cursor.iternext_dup(keys=True, values=True):
                    yield self.unpack_key(k), unpack_row(v)
        return None

    @profile_generator(MyProfileStat)
    def iter_scan(self, key_lower: Any = None) -> Generator[tuple, None, None]:
        with self.lmdb_tx.cursor(db=self.lmdb_handle) as cursor:
            if key_lower is not None:
                if not cursor.set_range(self.pack_key(key_lower)):
                    # nothing in range; need to return explicitly or else lmdb will scan from beginning:
                    return
            for k, v in cursor:
                yield self.unpack_key(k), unpack_row(v)
        return

    @profile(MyProfileStat)
    def put(self, key: Any, row: tuple) -> None:
        assert self.lmdb_tx.put(self.pack_key(key), pack_row(row), overwrite=True, db=self.lmdb_handle)
        return

    @profile(MyProfileStat)
    def delete(self, key: Any, row: tuple | None = None) -> int:
        with self.lmdb_tx.cursor(db=self.lmdb_handle) as cursor:
            if self.unique:
                if cursor.set_key(self.pack_key(key)):
                    if row is None or row == unpack_row(cursor.value()):
                        assert cursor.delete()
                        return 1
                return 0
            elif row is None: # delete all with the same key
                if cursor.set_key(self.pack_key(key)):
                    count = cursor.count()
                    for i in range(count):
                        assert cursor.delete()
                    return count
                return 0
            else: # delete the entry matching both key and row, if any
                if cursor.set_key_dup(self.pack_key(key), pack_row(row)):
                    assert cursor.delete()
                    return 1
                return 0

    def _close(self):
        """NOTE: lmdb's Python binding designers decided not to expose the ability
        to close lmdb file handles in API (underlying lmdb actually reuse handles
        across transactions, so there is a possibility of inadvertently
        closing handles while they are still used by others).
        However, leaving all handles around is known to cause performance issues.
        There isn't much we could do here for now, but we will keep an eye on it.
        """
        self.lmdb_handle = None
        return

class LMDBStorageManager(StorageManager):
    """LMDB-based storage manager.
    """

    def __init__(self, location: str, tmp_location: str):
        """
        """
        super().__init__()
        self.location: Final = location
        self.tmp_location: Final = tmp_location
        self.env: Final = lmdb.open(self.location, map_size=globals.MAX_DB_SIZE, max_dbs=globals.MAX_FILES)
        self.tmp_env: Final = lmdb.open(self.tmp_location, map_size=globals.MAX_DB_SIZE, max_dbs=globals.MAX_FILES)
        return

    def heap_file(self,
                  tx: Transaction,
                  name: str,
                  row_type: RowType,
                  create_if_not_exists: bool = False
    ) -> HeapFile:
        if not isinstance(tx, LMDBTransactionInterface):
            raise StorageMangerException('unexpected error')
        f = LMDBHeapFile(self, tx, name, row_type)
        f._open(create_if_not_exists=create_if_not_exists)
        return f

    def delete_heap_file(self, tx: Transaction, name: str) -> int:
        if not isinstance(tx, LMDBTransactionInterface):
            raise StorageMangerException('unexpected error')
        try:
            f = LMDBHeapFile(self, tx, name, []) # types don't matter here, just trying to drop it
            f._open()
            tx.lmdb_tx.drop(f.lmdb_handle, delete=True)
            f._close()
            return 1
        except lmdb.NotFoundError:
            return 0

    def bplus_tree(self,
                   tx: Transaction,
                   name: str,
                   key_type: ValType,
                   row_type: RowType,
                   unique: bool = False,
                   create_if_not_exists: bool = False
    ) -> BplusTree:
        if not isinstance(tx, LMDBTransactionInterface):
            raise StorageMangerException('unexpected error')
        f = LMDBBplusTree(self, tx, name, key_type, row_type, unique=unique)
        f._open(create_if_not_exists=create_if_not_exists)
        return f

    def delete_bplus_tree(self, tx: Transaction, name: str) -> int:
        if not isinstance(tx, LMDBTransactionInterface):
            raise StorageMangerException('unexpected error')
        try:
            f = LMDBBplusTree(self, tx, name, ValType.VARCHAR, []) # types don't matter here, just trying to drop it
            f._open()
            tx.lmdb_tx.drop(f.lmdb_handle, delete=True)
            f._close()
            return 1
        except lmdb.NotFoundError:
            return 0

    def shutdown(self) -> None:
        return
