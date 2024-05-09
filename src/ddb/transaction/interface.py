from typing import final, Final, Self, Generic, TypeVar, Iterable
from abc import ABC, abstractmethod
from enum import Enum, auto

import logging

class TransactionException(Exception):
    """Exceptions thrown by functions in the :mod:`.transaction` package.
    """
    pass

class TransactionStatus(Enum):
    """Status of a transaction.
    """
    ACTIVE = auto()
    COMMITTED = auto()
    ABORTED = auto()

T = TypeVar('T', bound='Transaction')
"""Type variable specifying the type of transaction used by :class:`.TransactionManager`.
"""

class TransactionManager(ABC, Generic[T]):
    """An abstract base class for ``TransactionManager``,
    which manages transactions (of some subtype ``T`` of :class:`.Transaction`).
    An implementation of this class should aim to enscapsulate all transaction processing logic therein.

    NOTE: There are two features worth noting about our transaction interface.
    First, we allow nested transactions.
    Committing/aborting an inner transaction doesn't commit/abort its enclosing transaction;
    aborting a transaction menas all work done by itself and its inner transactions will be aborted
    (even if the inner transactions committed);
    committing a top-level transaction means work done by all its committed inner transactions will be made permanent.
    Second, we allow some transactions to execute in a separate "tmp" space.
    This design is motivated by the need to support situations where,
    say, a read-only sort-merge join on the database may require writing to the tmp space.
    Here, we cannot nest a read/write transaction inside a read-only transaction,
    so instead, the query uses two parallel transactions:
    one read-only on the database, and the other read/write on the tmp space.
    """

    @abstractmethod
    def begin_transaction(self, parent: T | None = None, read_only: bool = False, tmp: bool = False) -> T:
        """Begin a new transaction.
        If ``parent`` is given, the new transaction will be nested therein.
        If ``tmp`` is set, the new transaction will be operating in the separate tmp space.
        """
        pass

    @abstractmethod
    def is_tmp(self, tx: T) -> bool:
        """Check whether ``tx`` is in the tmp space.
        """

    @abstractmethod
    def get_parent(self, tx: T) -> T | None:
        """Return the enclosing transaction of ``tx`` if it is nested, or ``None`` otherwise.
        """
        pass

    @abstractmethod
    def get_children(self, tx: T) -> Iterable[T]:
        """Return the transactions nested immediately within ``tx``.
        """
        pass

    @abstractmethod
    def get_status(self, tx: T) -> TransactionStatus:
        """Return the status of ``tx``.
        """
        pass

    @abstractmethod
    def commit(self, tx: T) -> None:
        """Commit transaction ``tx``.  It must be active and it must not have an active nested transaction."""
        pass

    @abstractmethod
    def abort(self, tx: T) -> None:
        """Abort transaction ``tx``.  It must be active.
        If it has an active nested transaction, that transaction will be abort first.
        """
        pass

class Transaction(ABC):
    """A abstract base class for transactions.
    This is intended as a lightweight class;
    most logic concerning transactions should be implemented in :class:`.TransactionManager`.
    """

    @abstractmethod
    def __init__(self, tm: TransactionManager, id: int, read_only: bool = False) -> None:
        """Constructor for a transaction, with minimally required fields.
        Subclass should override (but also invoke the provided constructor).
        In general, this constructor should only be invoked by a :class:`.TransactionManager`:
        new transactions should be created by a :class:`.TransactionManager`
        or as a nested transaction within an existing one.
        """
        self.tm: Final = tm
        self.id: Final[int] = id
        self.read_only: Final[bool] = read_only
        return

    @final
    def is_tmp(self) -> bool:
        """Check whether this transaction is in the tmp space.
        """
        return self.tm.is_tmp(self)

    @final
    def get_status(self) -> TransactionStatus:
        """Return the transaction status.
        """
        return self.tm.get_status(self)

    @final
    def begin_nested(self, read_only: bool = False) -> Self:
        """Create a new transaction nested within this one.
        """
        return self.tm.begin_transaction(parent = self, read_only = read_only, tmp = self.is_tmp())

    @final
    def get_parent(self) -> Self | None:
        """Return the enclosing transaction if this one is nested, or ``None`` otherwise.
        """
        return self.tm.get_parent(self)

    @final
    def get_children(self) -> Iterable[Self]:
        """Return the transactions nested immediately within this one.
        """
        return self.tm.get_children(self)

    @final
    def commit(self) -> None:
        """Commit this transaction.
        """
        self.tm.commit(self)
        return

    @final
    def abort(self) -> None:
        """Abort this transaction.
        """
        self.tm.abort(self)
        return

    def __str__(self) -> str:
        s = f'{self.id}'
        if (p := self.get_parent()) is not None:
            s += ' (sub of {})'.format(p.id)
        return s

    def __enter__(self) -> Self:
        """Required for the context manager to ready this object.
        """
        logging.debug(f'entering transaction {self}')
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        """Required for the context manager to release any resources used by this object.
        """
        logging.debug(f'exiting transaction {self}')
        if self.get_status() == TransactionStatus.ACTIVE:
            logging.debug(f'aborting transaction {self}')
            self.tm.abort(self)
        return
