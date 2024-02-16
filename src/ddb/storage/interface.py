"""This module mostly defines *abstract* classes and documents the storage API.
See the :mod:`.lmdb` module for implementation classes.
"""
from typing import final, Self, Final, Iterable, Generator, Any
from abc import ABC, abstractmethod

from ..types import ValType, RowType
from ..transaction import Transaction

class StorageMangerException(Exception):
    """Exceptions thrown by functions in the :mod:`.storage` package.
    """
    pass

class HeapFile(ABC):
    """A ``HeapFile`` stores rows (tuples) that are uniquely identfied by row ids (integers).

    The row ids are internal, immutable, and not included in rows themselves.
    You can get, put (upsert), and delete rows by row ids.
    You can also scan all rows or batch append new rows.
    There is no guarantee on whether the old row ids of deleted rows will be reused later.
    New rows will be automatically assigned new row ids, which are not necessarily consecutive or increasing.

    This class is implemented as a Python "context manager", which makes it easy to allocate and release resources.
    Also, instead of constructing a ``HeapFile`` directly, consider doing so through :meth:`.StorageManager.heap_file`.
    For example:

    .. code-block:: python

        with storage_manager.heap_file(tx, 'mytable', row_type) as h:
            for k, v in h.iter_scan():
                print(f'{k}: {v}')

    Attributes:
        name: name of the heap file;
            caller is responsible for ensuring that the name is unique among all files managed by the storage manager.
        row_type: row type for all rows (excluding the internal row id).
    """

    @abstractmethod
    def __init__(self, tx: Transaction, name: str, row_type: RowType):
        """Called by implementation class to help initalize an object of that class.
        (You shouldn't instantiate this abstract class directly.)

        For documentation on input arguments, see corresponding member attributes.

        This method should not and will not open the heap file;
        :meth:`.HeapFile._open` is used for that purpose instead.
        """
        self.tx: Final = tx
        self.name: Final = name
        self.row_type: Final = row_type
        return

    @abstractmethod
    def _open(self, create_if_not_exists: bool = False) -> None:
        """Called by the context manager method :meth:`.HeapFile.__enter__`
        to ready this object for managing the underlying file.
        The implementation class should implement this method.
        """
        pass

    @final
    def __enter__(self) -> Self:
        """Required for the context manager to ready this object."""
        self._open()
        return self
    
    @abstractmethod
    def get(self, row_id: int) -> tuple | None:
        """Return the row with the given ``row_id``, or ``None`` if not found."""
        pass

    @abstractmethod
    def iter_scan(self, return_row_id: bool = False) -> Generator[tuple, None, None]:
        """Return a Python generator that iterates over all rows in the heap file.
        If ``return_row_id`` is ``True``, then return the (row id, row) pairs instead."""
        pass

    @abstractmethod
    def put(self, row: tuple, row_id: int | None = None) -> int:
        """Store the given row in the heap file, and return the row id associated with it.
        If ``row_id`` is ``None``, the method will automatically generate one.
        Otherwise, the given ``row_id`` will be used, and if there is already a row with this id, it will be overwritten.
        """
        pass

    @abstractmethod
    def batch_append(self, rows: Iterable[tuple]) -> tuple[int, int]:
        """Append a list of rows to the end of heap file, assigned with consecutive row ids;
        return a pair (``row_id_start``, ``num_written``), where
        ``row_id_start`` is the id of the first row appended (if any), and
        ``num_written`` is the number of rows written.
        """
        pass

    @abstractmethod
    def truncate(self) -> int:
        """Truncate the file (clearing its rows), and
        return the number of rows deleted.
        """
        pass

    @abstractmethod
    def delete(self, row_id: int) -> int:
        """Delete the row with the given id (if any) from the heap file, and
        return the number of rows deleted (should be either ``1`` or ``0``).
        """
        pass

    @abstractmethod
    def stat(self) -> dict:
        """Return various statistics associated with this heap file,
        such as the number of rows and blocks, etc.
        """
        pass

    @abstractmethod
    def _close(self):
        """Called by the context manager method :meth:`.HeapFile.__exit__`
        to release any resources used by this object to manage the underlying file.
        The implementation class should implement this method.
        """
        pass

    @final
    def __exit__(self, exception_type, exception_value, exception_traceback):
        """Required for the context manager to release any resources used by this object."""
        self._close()
        return

class BplusTree(ABC):
    """A ``BplusTree`` stores a collection of (key, row) entries sorted by key,
    allowing fast retrieval and range scans of rows by key.

    The data structure enforces that no entries sharing the same key have identical row values.
    If ``unique`` is ``True``, it will additionally enforce uniqueness of key values across all entries.

    You can get one row or all rows associated with the same key,
    or you can scan all rows in a key range (starting with an optional lower bound).
    You can put a (key, row) pair ---
    if ``unique`` is ``True``, this will overwrites any existing entry with the same key;
    otherwise, this adds a new entry unless the exact same (key, row) entry already exists.
    You can also delete a (key, row) pair, or all entries with a given key.

    This class is implemented as a Python "context manager", which makes it easy to allocate and release resources.
    Also, instead of constructing a ``BplusTree`` directly, consider doing so through :meth:`.StorageManager.bplus_tree`.
    For example:

    .. code-block:: python

        with storage_manager.bplus_tree(tx, 'mytable', name, ValType.INTEGER, row_type) as h:
            for k, v in h.iter_scan(key_lower=100):
                print(f'{k}: {v}')

    Attributes:
        name: name of the B+tree file;
            caller is responsible for ensuring that the name is unique among all files managed by the storage manager.
        key_type: type of the key; mutiple-component keys are currently not supported.
        row_type: TBD.
        unique: if `True`, the B+tree will enforce uniqueness of key values.
    """

    @abstractmethod
    def __init__(self, tx: Transaction, name: str,
                 key_type: ValType, row_type: RowType,
                 unique: bool = False):
        """Called by implementation class to help initalize an object of that class.
        (You shouldn't instantiate this abstract class directly.)

        For documentation on input arguments, see corresponding member attributes.

        This method should not and will not open the B+tree;
        :meth:`.BplusTree._open` is used for that purpose instead.
        """
        self.tx: Final = tx
        self.name: Final = name
        self.key_type: Final = key_type
        self.row_type: Final = row_type
        self.unique: Final = unique
        return

    @abstractmethod
    def _open(self, create_if_not_exists: bool = False) -> None:
        """Called by the context manager method :meth:`.BplusTree.__enter__` to ready the resource.
        to ready this object for managing the underlying file.
        The implementation class should implement this method.
        """
        pass

    @final
    def __enter__(self) -> Self:
        """Required for the context manager to ready this object."""
        self._open()
        return self

    @abstractmethod
    def get_one(self, key: Any) -> tuple | None:
        """Return the first row with the given ``row_id``, or ``None`` if not found."""
        pass

    @abstractmethod
    def iter_get(self, key: Any) -> Generator[tuple, None, None]:
        """Return a Python generator that iterates over all (key, row) entries with given ``key``."""
        pass

    @abstractmethod
    def iter_scan(self, key_lower: Any = None) -> Generator[tuple, None, None]:
        """Return a Python generator that iterates over all (key, row) entries where ``key >= key_lower``.
        If ``key_lower`` is ``None``, however, iterate from the beginning.

        While this method does not allow an upper bound (i.e., ``key <= key_upper``)
        or a non-inclusive bound (e.g., ``key > key_lower_x``) to be specified,
        caller can easily perform such checks themselves.
        For example, the following scan the key range ``(100, 200]``:

        .. code-block:: python

            for k, v in bplus_tree.iter_scan(100):
                if k <= 100:
                    continue
                elif k > 200:
                    break
                print(k, v)

        With the above code, Python will garbage-collect the generator *eventually*.
        For efficiency, however, consider closing the generator explicitly using the following pattern:

        .. code-block:: python

            from contextlib import closing
            with closing(bplus_tree.iter_scan(100)) as iter:
                for k, v in iter:
                    if k <= 100:
                        continue
                    elif k > 200:
                        break
                    print(k, v)

        """
        pass

    @abstractmethod
    def put(self, key: Any, row: tuple) -> None:
        """Store the given (key, row) entry in the B+tree.
        If ``unique`` is ``True``, this will overwrites any existing entry with the same key;
        otherwise, this adds a new entry unless the exact same (key, row) entry already exists.
        """
        pass

    @abstractmethod
    def delete(self, key: Any, row: tuple | None = None) -> int:
        """ Delete the given (key, row) pair (if it exists), or,
        if ``row`` is ``None``, all entries with the given key.
        Return the number of entries deleted.
        """
        pass

    @abstractmethod
    def stat(self) -> dict:
        """Return various statistics associated with this B+tree,
        such as tree height, number of entries, number of internal/leaf blocks, etc.
        """
        pass

    @abstractmethod
    def _close(self):
        """Called by the context manager method :meth:`.BplusTree.__exit__`
        to release any resources used by this object to manage the underlying file.
        The implementation class should implement this method.
        """
        pass

    @final
    def __exit__(self, exception_type, exception_value, exception_traceback):
        """Required for the context manager to release any resources used by this object."""
        self._close()
        return

class StorageManager(ABC):
    @abstractmethod
    def __init__(self):
        return

    @abstractmethod
    def heap_file(self,
                  tx: Transaction,
                  name: str,
                  row_type: RowType,
                  create_if_not_exists: bool = False
    ) -> HeapFile:
        """Return a heap file, already opened for operations.
        """
        pass

    @abstractmethod
    def delete_heap_file(self, tx: Transaction, name: str) -> int:
        pass

    @abstractmethod
    def bplus_tree(self,
                   tx: Transaction,
                   name: str,
                   key_type: ValType,
                   row_type: RowType,
                   unique: bool = False,
                   create_if_not_exists: bool = False
    ) -> BplusTree:
        """Return a B+tree, already opened for operations.
        """
        pass

    @abstractmethod
    def delete_bplus_tree(self, tx: Transaction, name: str) -> int:
        pass

    @abstractmethod
    def shutdown(self) -> None:
        pass
