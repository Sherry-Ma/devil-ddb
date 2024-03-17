from typing import TypeVar, Generic, Final, Iterable, TYPE_CHECKING
from abc import ABC, abstractmethod
from dataclasses import dataclass
from math import ceil

from ..globals import BLOCK_SIZE

if TYPE_CHECKING:
    # this hack and the use of quoted types for forward references below
    # are required to avoid Python circular import nightmare.
    from ..storage import StorageManager
    from ..metadata import MetadataManager, BaseTableMetadata
    from ..validator import ValExpr, valexpr
    from ..executor import StatementContext

@dataclass
class TableStats:
    """Statistics about a specific table instance (e.g., output from a :class:`.QPop`).
    This is really bare-bone; make a subclass to extend it.
    """
    row_count: int
    """Number of rows.
    """
    row_size: int
    """Average size of a row, in bytes.
    This may be bigger than the sum of column sizes because of additional header information.
    """
    column_sizes: list[int]
    """Average size of each column (identified by its index), in bytes.
    """
    tree_height: int | None
    """Height of the data structure storing records on disk.
    Only applicable when we are reading from a heap or index file,
    where every lookup or scan will incur this number of block reads at a minimum.
    """
    fill_factor: float | None
    """How densely packed the records are on disk (between 0.0 to 1.0).
    Only applicable when we are reading from a heap or index file.
    """

    def block_count(self) -> int:
        return ceil(self.row_count * self.row_size /\
                    (BLOCK_SIZE * (1 if self.fill_factor is None or self.fill_factor == 0 else
                                   self.fill_factor)))

    def pstr(self) -> Iterable[str]:
        yield f'output rows: {self.row_count} = {self.block_count()} blocks @{self.row_size}B/row'
        yield 'column sizes: ({})'.format(', '.join(str(size) for size in self.column_sizes))
        if self.fill_factor is not None or self.tree_height is not None:
            yield f'storage: tree height {self.tree_height}; fill factor: {self.fill_factor}'
        return

@dataclass
class CollectionStats:
    """Statics about a database instance or a collection of table instances.
    This is just a placeholder; make a subclass to extend it.
    """
    def pstr(self) -> Iterable[str]:
        yield from ()
        return

TZ = TypeVar('TZ', bound='TableStats')
"""Type variable specifying the type of table stats objects used by :class:`.StatsManager`.
"""

CZ = TypeVar('CZ', bound='CollectionStats')
"""Type variable specifying the type of collection stats objects used by :class:`.StatsManager`.
"""

class StatsManager(ABC, Generic[TZ, CZ]):
    """The stats manager.
    """
    def __init__(self, sm: 'StorageManager', mm: 'MetadataManager') -> None:
        self.sm: Final = sm
        self.mm: Final = mm
        return

    @abstractmethod
    def analyze_stats(self, context: 'StatementContext', base_metas: list['BaseTableMetadata'] | None = None) -> CZ:
        """Compute and return stats for the collection of tables (and indexes on them), from scratch.
        If ``base_metas`` is ``None``, we will do so for the whole database.
        This method guarantees the freshness of returned stats.
        """
        pass

    @abstractmethod
    def base_table_stats(self, context: 'StatementContext', meta: 'BaseTableMetadata', return_row_id: bool = False) -> TZ:
        """Estimate stats for base table with given metadata.
        Columns and their ordering are consistent with those obtained by :class:`.TableScanPop`.
        """
        pass

    @abstractmethod
    def secondary_index_stats(self, context: 'StatementContext', meta: 'BaseTableMetadata', column_name: str) -> TZ:
        """Estimate stats for a secondary index for table with given metadata on column with the given name.
        """
        pass

    @abstractmethod
    def literal_table_stats(self, rows: list[tuple]) -> TZ:
        """Estimate stats for a literal table.
        """
        pass

    @abstractmethod
    def selection_stats(self, stats: TZ, cond: 'ValExpr | None') -> TZ:
        """Estimate stats for the output of a selection with filter condition ``cond``
        over the input table with ``stats``.
        We assume that all column references in ``cond`` are :class:`.RelativeColumnRef` objects.
        """
        pass

    @abstractmethod
    def projection_stats(self, stats: TZ, exprs: list['ValExpr']) -> TZ:
        """Estimate stats for the output of a projection with
        output columns computed by ``exprs`` over the input table with ``stats``.
        We assume that all column references in ``exprs`` are :class:`.RelativeColumnRef` objects.
        The projection here is duplicate-preserving.
        For duplicate-eliminating projection estimation, see :meth:`grouping_stats`.
        """
        pass

    @abstractmethod
    def grouping_stats(self, stats: TZ,
                       grouping_exprs: list['ValExpr'],
                       aggr_exprs: list['valexpr.AggrValExpr']) -> TZ:
        """Estimate stats for a grouping+aggregation operation specified by ``grouping_exprs`` and ``aggr_exprs``
        over the input table with ``stats``.
        We assume that all column references in ``grouping_exprs`` and ``aggr_exprs`` are :class:`.RelativeColumnRef` objects.
        """
        pass

    @abstractmethod
    def join_stats(self, left_stats: TZ, right_stats: TZ, cond: 'ValExpr | None') -> TZ:
        """Estimate stats for the output of a join with join condition ``cond``
        over left and right inputs with respective stats.
        We assume that all column references in ``cond`` are :class:`.RelativeColumnRef` objects.
        """
        pass

    @abstractmethod
    def tweak_stats(self, stats: TZ, row_count: int) -> TZ:
        """Return a copy of the given stats object with a new row count.
        Any estimates pertaining to a heap/index file will be removed.
        """
        pass