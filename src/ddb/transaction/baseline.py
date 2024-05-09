from typing import Final, Iterable
import lmdb # type: ignore
import time

from ..storage.lmdb import LMDBStorageManager
from ..storage.lmdb import LMDBTransactionInterface
from .interface import TransactionStatus, TransactionManager, TransactionException

class LMDBTransaction(LMDBTransactionInterface):
    def __init__(self, tm: 'LMDBTransactionManager', id: int, lmdb_tx: lmdb.Transaction,
                 parent: 'LMDBTransaction | None' = None, read_only: bool = False) -> None:
        super().__init__(tm, id, lmdb_tx, read_only=read_only)
        self.parent: Final['LMDBTransaction | None'] = parent
        if self.parent is not None:
            self.parent.children.append(self)
        self.children: Final[list['LMDBTransaction']] = list()
        self.status: TransactionStatus = TransactionStatus.ACTIVE
        return

class LMDBTransactionInTmp(LMDBTransaction):
    pass

class LMDBTransactionManager(TransactionManager[LMDBTransaction]):
    def __init__(self, sm: LMDBStorageManager) -> None:
        self.sm: Final = sm
        return

    def begin_transaction(self, parent: LMDBTransaction | None = None, read_only: bool = False, tmp: bool = False) -> LMDBTransaction:
        if parent is not None:
            if parent.read_only and not read_only:
                raise TransactionException(f'cannot nest read/write transaction in ready-only transaction {parent.id}')
            if parent.is_tmp() and not tmp:
                raise TransactionException(f'cannot nest a regular transaction in in-tmp transaction {parent.id}')
            if tmp and not parent.is_tmp():
                raise TransactionException(f'cannot nest an in-tmp transaction in regular transaction {parent.id}')
        if tmp:
            lmdb_tx = self.sm.tmp_env.begin(parent = parent.lmdb_tx if parent is not None else None,
                                            write = not(read_only))
            return LMDBTransactionInTmp(self, time.monotonic_ns(), lmdb_tx, parent = parent, read_only = read_only)
        else:
            lmdb_tx = self.sm.env.begin(parent = parent.lmdb_tx if parent is not None else None,
                                        write = not(read_only))
            return LMDBTransaction(self, time.monotonic_ns(), lmdb_tx, parent = parent, read_only = read_only)

    def is_tmp(self, tx: LMDBTransaction) -> bool:
        return isinstance(tx, LMDBTransactionInTmp)

    def get_parent(self, tx: LMDBTransaction) -> LMDBTransaction | None:
        return tx.parent

    def get_children(self, tx: LMDBTransaction) -> Iterable[LMDBTransaction]:
        return tx.children

    def get_status(self, tx: LMDBTransaction) -> TransactionStatus:
        return tx.status

    def commit(self, tx: LMDBTransaction) -> None:
        if tx.status != TransactionStatus.ACTIVE:
            raise TransactionException(f'cannot commit inactive transaction {tx.id}')
        for child in tx.children:
            if child.status == TransactionStatus.ACTIVE:
                raise TransactionException(f'cannot commit transaction {tx.id} with active nested transaction {child.id}')
        tx.lmdb_tx.commit()
        tx.status = TransactionStatus.COMMITTED
        return

    def abort(self, tx: LMDBTransaction) -> None:
        if tx.status != TransactionStatus.ACTIVE:
            raise TransactionException(f'cannot abort inactive transaction {tx.id}')
        for child in tx.children:
            if child.status == TransactionStatus.ACTIVE:
                self.abort(child)
        tx.lmdb_tx.abort()
        tx.status = TransactionStatus.ABORTED
        return
