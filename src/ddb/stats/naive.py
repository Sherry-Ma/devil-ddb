from typing import Iterable
from dataclasses import dataclass
from datasketches import cpc_sketch # type: ignore
from copy import deepcopy
from sys import getsizeof

from ..storage import StorageManager
from ..types import ValType, column_sizes, row_size
from ..metadata import MetadataManager, BaseTableMetadata, INTERNAL_ROW_ID_COLUMN_TYPE
from ..validator import ValExpr, valexpr
from ..executor import ExecutorException, TableScanPop, StatementContext

from .interface import TableStats, CollectionStats, StatsManager

@dataclass
class NaiveTableStats(TableStats):
    """A crude implementation that tries to estimate the number distinct values for each column.
    But it does not employ histograms or track joint distribution across columns.
    """
    distinct_counts: list[int]
    """Number of distinct values for a given column (identified by its index).
    """

    def pstr(self) -> Iterable[str]:
        yield from super().pstr()
        yield 'distinct counts for each column: ' + ', '.join('?' if c is None else str(c) for c in self.distinct_counts)
        return

@dataclass
class NaiveCollectionStats(CollectionStats):
    """A crude implementation that tracks statistics for each table independently.
    A more sophisticated implementation would provide correlation statistics across tables.
    """
    table_stats: dict[str, NaiveTableStats]
    """A mapping from table names to their stats.
    """
    index_stats: dict[str, dict[str, NaiveTableStats]]
    """A mapping from (table name, column name) to the stats of the associated secondary index.
    """

    def pstr(self) -> Iterable[str]:
        for name, stats in self.table_stats.items():
            yield f'{name}:'
            for s in stats.pstr():
                yield '| ' + s
            for column_name, stats in self.index_stats[name].items():
                yield f'|- {name}({column_name}):'
                for s in stats.pstr():
                    yield '|  | ' + s
            yield '\\____'
        return

class NaiveStatsManager(StatsManager[NaiveTableStats, NaiveCollectionStats]):
    def __init__(self, sm: StorageManager, mm: MetadataManager) -> None:
        super().__init__(sm, mm)
        self.stats: NaiveCollectionStats = NaiveCollectionStats(table_stats=dict(), index_stats=dict())
        return

    def analyze_stats(self, context: StatementContext, base_metas: list[BaseTableMetadata] | None = None) -> NaiveCollectionStats:
        for meta in (base_metas if base_metas is not None else self.mm.list_base_tables(context.tx)):
            self.base_table_stats(context, meta, refresh=True)
        return self.stats # this may be more than what caller asked for, but the extra info (possibly stale) doesn't hurt

    @classmethod
    def _update_stats_from_lmdb(cls, table_stats: NaiveTableStats, lmdb_stats: dict) -> None:
        bytes_on_disk = lmdb_stats['leaf_pages'] * lmdb_stats['psize']
        table_stats.row_count = lmdb_stats['entries']
        # leave table_stats.row_size unchanged
        # leave table_stats.column_sizes unchanged
        table_stats.tree_height = lmdb_stats['depth']
        table_stats.fill_factor = min(1.0, float(bytes_on_disk) / float(table_stats.row_count * table_stats.row_size + 1))
        # leave table_stats.distinct_counts unchanged
        return

    @classmethod
    def _distinct_counts_from_rows(cls, rows: Iterable[tuple]) -> list[int]:
        sketches: list[cpc_sketch] = []
        count = 0
        for row in rows:
            if sketches == []: # use the first row to infer format
                sketches = [ (cpc_sketch() if type(val) in (int, str, float) else None) for val in row ]
            for i, val in enumerate(row):
                if (sketch := sketches[i]) is not None:
                    sketch.update(val)
            count += 1
        return [
            (min(max(round(sketch.get_estimate()), 1), count) if sketch is not None else count)
            for sketch in sketches
        ]

    def base_table_stats(self, context: StatementContext, meta: BaseTableMetadata,
                         return_row_id: bool = False,
                         refresh: bool = False) -> NaiveTableStats:
        if not refresh and meta.name in self.stats.table_stats:
            if return_row_id:
                new_stats = deepcopy(self.stats.table_stats[meta.name])
                new_stats.row_size += INTERNAL_ROW_ID_COLUMN_TYPE.size
                new_stats.column_sizes.insert(0, INTERNAL_ROW_ID_COLUMN_TYPE.size)
                new_stats.distinct_counts.insert(0, new_stats.row_count)
                return new_stats
            else:
                return self.stats.table_stats[meta.name]
        table_stats = NaiveTableStats(
            row_count = 0,
            row_size = row_size(meta.column_types),
            column_sizes = column_sizes(meta.column_types),
            tree_height = 0,
            fill_factor = 0.0,
            distinct_counts = [0] * len(meta.column_names))
        with self.mm.table_storage(context.tx, meta) as f:
            storage_stats = f.stat()
            type(self)._update_stats_from_lmdb(table_stats, storage_stats)
        if table_stats.row_count > 0:
            # use a TableScanPop to compute column stats for each column, and to get desired column ordering:
            # we will ignore return_row_id for now, just to create a base object to be cached. 
            scan = TableScanPop(context, meta.name, meta)
            table_stats.distinct_counts = type(self)._distinct_counts_from_rows(scan.execute())
            if meta.primary_key_column_index is not None: # but we know the primary key count for sure!
                table_stats.distinct_counts[0] = table_stats.row_count # should be the first column returned by TableScanPop
        # cache it!
        self.stats.table_stats[meta.name] = table_stats
        # while we are at it, just refresh stats about this table's indexes too:
        self.stats.index_stats[meta.name] = dict()
        for si in meta.secondary_column_indices:
            column_name = meta.column_names[si]
            index_row_type = [meta.column_types[si], ValType.INTEGER]
            index_stats = NaiveTableStats(
                row_count = table_stats.row_count,
                row_size = row_size(index_row_type),
                column_sizes = column_sizes(index_row_type),
                tree_height = 0,
                fill_factor = 0.0,
                distinct_counts = [table_stats.distinct_counts[si], table_stats.row_count])
            with self.mm.index_storage(context.tx, meta, si) as f:
                storage_stats = f.stat()
                type(self)._update_stats_from_lmdb(index_stats, storage_stats)
            self.stats.index_stats[meta.name][column_name] = index_stats
        # now that refreshing is done, run through this method again to handle return_row_id as needed:
        return self.base_table_stats(context, meta, return_row_id=return_row_id, refresh=False)

    def secondary_index_stats(self, context: StatementContext, meta: BaseTableMetadata, column_name: str) -> NaiveTableStats:
        if meta.name not in self.stats.index_stats or column_name not in self.stats.index_stats[meta.name]:
            self.base_table_stats(context, meta, refresh=True)
        return self.stats.index_stats[meta.name][column_name]

    def literal_table_stats(self, rows: list[tuple]) -> NaiveTableStats:
        return NaiveTableStats(
            row_count = len(rows),
            row_size = getsizeof(rows[0]),
            column_sizes = [getsizeof(val) for val in rows[0]],
            tree_height = 0,
            fill_factor = 0.0,
            distinct_counts = type(self)._distinct_counts_from_rows(rows))

    def selection_stats(self, stats: NaiveTableStats, cond: ValExpr | None) -> NaiveTableStats:
        new_stats = deepcopy(stats)
        new_stats.tree_height = None
        new_stats.fill_factor = None
        if cond is None:
            return new_stats
        selectivity: float = 1.0
        for e in valexpr.conjunctive_parts(cond):
            if (triple := valexpr.is_column_comparing_to_literal(e)) is not None:
                col, comp, val = triple
                if not isinstance(col, valexpr.leaf.RelativeColumnRef) or col.input_index != 0:
                    raise ExecutorException('unexpected error')
                distinct_count = new_stats.distinct_counts[col.column_index]
                if comp == valexpr.binary.EQ:
                    new_distinct_count = 1
                elif comp == valexpr.binary.NE:
                    new_distinct_count = max(distinct_count-1, 1)
                else: # a wild guess
                    new_distinct_count = int(max(distinct_count/3, 1))
                new_stats.distinct_counts[col.column_index] = new_distinct_count
                selectivity = selectivity * new_distinct_count / distinct_count
            else: # a wild guess
                # hard to say distinct counts are affected, so assume preservation of value sets by default,
                # but still apply an overall reduction:
                selectivity = selectivity / 3
        # apply the final selectivity and make sure no distinct counts exceed the new row count:
        new_stats.row_count = int(max(round(new_stats.row_count * selectivity), 1))
        for i in range(len(new_stats.distinct_counts)):
            if new_stats.distinct_counts[i] > new_stats.row_count:
                new_stats.distinct_counts[i] = new_stats.row_count
        return new_stats

    def projection_stats(self, stats: NaiveTableStats, exprs: list[ValExpr]) -> NaiveTableStats:
        new_stats = NaiveTableStats(
            row_count = stats.row_count,
            row_size = 0,
            column_sizes = list(),
            tree_height = None,
            fill_factor = None,
            distinct_counts = list()
        )
        for e in exprs:
            column_size: int
            distinct_count: int
            if isinstance(e, valexpr.leaf.RelativeColumnRef):
                column_size = stats.column_sizes[e.column_index]
                distinct_count = stats.distinct_counts[e.column_index]
            elif valexpr.in_scope(e, []):
                column_size = getsizeof(valexpr.eval_literal(e))
                distinct_count = 1
            else:
                column_size = e.valtype().size
                distinct_count = 1
                for column_ref in valexpr.find_column_refs(e):
                    if isinstance(column_ref, valexpr.leaf.RelativeColumnRef):
                        distinct_count = distinct_count * stats.distinct_counts[column_ref.column_index]
            new_stats.column_sizes.append(column_size)
            new_stats.distinct_counts.append(min(distinct_count, stats.row_count))
        new_stats.row_size = sum(new_stats.column_sizes)
        return new_stats

    def grouping_stats(self, stats: NaiveTableStats, grouping_exprs: list[ValExpr]) -> NaiveTableStats:
        # TODO: estimate # dstinct for each grouping expr.
        # output # is product of all distinct for grouping, but cap it to input size.
        raise NotImplementedError

    def join_stats(self, left_stats: NaiveTableStats, right_stats: NaiveTableStats, cond: ValExpr | None) -> NaiveTableStats:
        new_stats = NaiveTableStats(
            row_count = left_stats.row_count * right_stats.row_count,
            row_size = left_stats.row_size + right_stats.row_size,
            column_sizes = left_stats.column_sizes + right_stats.column_sizes,
            tree_height = None,
            fill_factor = None,
            distinct_counts = left_stats.distinct_counts + right_stats.distinct_counts
        )
        if cond is None:
            return new_stats
        col_right_offset = len(left_stats.column_sizes)
        selectivity: float = 1.0
        for e in valexpr.conjunctive_parts(cond):
            if (triple := valexpr.are_columns_joining(e)) is not None:
                col_left, comp, col_right = triple
                left_distinct_count = left_stats.distinct_counts[col_left.column_index]
                right_distinct_coount = right_stats.distinct_counts[col_right.column_index]
                if comp == valexpr.binary.EQ:
                    # assume containment of value sets:
                    selectivity = selectivity / max(left_distinct_count, right_distinct_coount, 1)
                    new_stats.distinct_counts[col_left.column_index] = min(left_distinct_count, right_distinct_coount)
                    new_stats.distinct_counts[col_right_offset + col_right.column_index] = min(left_distinct_count, right_distinct_coount)
                elif comp == valexpr.binary.NE:
                    # complement of the equality case:
                    selectivity = selectivity * (1 - 1/max(left_distinct_count, right_distinct_coount, 1))
                else: # a wild guess
                    selectivity = selectivity / 3
        # for other columns, assume preservation of value sets, but limit count to # of output rows estimated.
        # apply the final selectivity and make sure no distinct counts exceed the new row count:
        new_stats.row_count = int(max(round(new_stats.row_count * selectivity), 1))
        for i in range(len(new_stats.distinct_counts)):
            if new_stats.distinct_counts[i] > new_stats.row_count:
                new_stats.distinct_counts[i] = new_stats.row_count
        return new_stats

    def tweak_stats(self, stats: NaiveTableStats, row_count: int) -> NaiveTableStats:
        new_stats = deepcopy(stats)
        new_stats.tree_height = None
        new_stats.fill_factor = None
        # set new row count and make sure no distinct counts exceed it:
        new_stats.row_count = row_count
        for i in range(len(new_stats.distinct_counts)):
            if new_stats.distinct_counts[i] > new_stats.row_count:
                new_stats.distinct_counts[i] = new_stats.row_count
        return new_stats
