from typing import Iterable, Generator, Any
from contextlib import closing
from functools import cached_property
from math import ceil

from ..profile import profile_generator
from ..metadata import TableMetadata, BaseTableMetadata, ValType, INTERNAL_ROW_ID_COLUMN_NAME, INTERNAL_ROW_ID_COLUMN_TYPE
from ..storage import BplusTree, HeapFile
from ..validator import OutputLineage, valexpr

from .interface import QPop, StatementContext, ExecutorException

class IndexScanPop(QPop[QPop.CompiledProps]):
    """Index scan physical operator.
    The underlying index is either a :class:`.BplusTree` (serving either as a primary index or a secondary index),
    or a :class:`.HeapFile` (in which case search condition has to be a specific row id).
    In the case of a secondary index, the scan returns (key, row id) rows,
    with the second column named ``ddb.metadata.INTERNAL_ROW_ID_COLUMN_NAME``.
    The operator calls the storage manager to perform the scan,
    which essentially uses one memory block for buffering.
    Before calling this operator's ``execute()``, a scan range or key needs to be set.
    """
    def __init__(self, context: StatementContext,
                 alias: str, meta: BaseTableMetadata, key_name: str,
                 is_range: bool) -> None:
        """Construct an index scan for the database table whose metadata is given by ``meta``,
        with table alias ``alias``, using the index on column named ``key_name``.
        Note that ``key_name`` can be ``ddb.metadata.INTERNAL_ROW_ID_COLUMN_NAME`` for a table with no primary key.
        ``is_range`` indicates whether this operator will be used for a key range scan,
        as opposed to looking up record(s) by a single key value.
        """
        super().__init__(context)
        self.alias = alias
        self.meta = meta
        self.key_name = key_name
        self.is_range = is_range
        self.key_lower: Any = None
        self.key_upper: Any = None
        self.lower_exclusive: bool = False
        self.upper_exclusive: bool = False
        return

    def memory_blocks_required(self) -> int:
        return 1

    def children(self) -> tuple[QPop[QPop.CompiledProps], ...]:
        return tuple()

    def set_key(self, key: Any) -> None:
        """Set the target search key for the subsequent :meth:`.execute()` call.
        """
        self.key_lower = key
        self.key_upper = key
        self.lower_exclusive = False
        self.upper_exclusive = False
        return

    def set_range(self, key_lower: Any, key_upper: Any,
                  lower_exclusive: bool | None,
                  upper_exclusive: bool | None) -> None:
        """Set the scan range for the subsequent :meth:`.execute()` call.
        """
        self.key_lower = key_lower
        self.key_upper = key_upper
        self.lower_exclusive = False if lower_exclusive is None else lower_exclusive
        self.upper_exclusive = False if upper_exclusive is None else upper_exclusive
        return

    def pstr_more(self) -> Iterable[str]:
        yield f'AS {self.alias} using {self.meta.name}({self.key_name})'
        yield 'key range: {}{}, {}{}'.format('(' if self.lower_exclusive else '[',
                                             self.key_lower, self.key_upper,
                                             ')' if self.upper_exclusive else ']')
        return

    def is_by_row_id(self) -> bool:
        return self.key_name == INTERNAL_ROW_ID_COLUMN_NAME

    def is_by_primary_key(self) -> bool:
        return self.meta.primary_key_column_index is not None and \
            self.key_name == self.meta.column_names[self.meta.primary_key_column_index]

    @cached_property
    def compiled(self) -> QPop.CompiledProps:
        output_column_names: list[str]
        output_column_types: list[ValType]
        output_lineage: OutputLineage
        ordered_columns: list[int]
        ordered_asc: list[bool]
        unique_columns: set[int]
        if self.is_by_row_id() or self.is_by_primary_key(): # primary index scan or heap file scan by row id
            output_column_names = list()
            output_column_types = list()
            output_lineage = list()
            ordered_columns = list()
            ordered_asc = list()
            unique_columns = set()
            for i, (column_name, column_type) in enumerate(zip(self.meta.column_names, self.meta.column_types)):
                # primay key would be the first column that gets read out
                if i == self.meta.primary_key_column_index:
                    insert_i = 0 
                    ordered_columns = [0]
                    ordered_asc = [True]
                    unique_columns = {0}
                else:
                    insert_i = len(output_column_names)
                output_column_names.insert(insert_i, column_name)
                output_column_types.insert(insert_i, column_type)
                output_lineage.insert(insert_i, set(((self.alias, column_name), )))
        else: # secondary index scan
            output_column_names = [ self.key_name, self.meta.id_name() ]
            output_column_types = [ self.meta.column_types[self.meta.column_names.index(self.key_name)],
                                    self.meta.id_type() ]
            output_lineage = [set(((self.alias, self.key_name), )), set(((self.alias, self.meta.id_name()), ))]
            ordered_columns = [0]
            ordered_asc = [True]
            unique_columns = {1} # while key may not be unique, the internal row id is
        return QPop.CompiledProps(
            output_metadata = TableMetadata(output_column_names, output_column_types),
            output_lineage = output_lineage,
            ordered_columns = ordered_columns,
            ordered_asc = ordered_asc,
            unique_columns = unique_columns)

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        if self.is_by_row_id():
            index_stats = self.context.zm.base_table_stats(self.context, self.meta)
            column_type = INTERNAL_ROW_ID_COLUMN_TYPE
            new_stats = self.context.zm.tweak_stats(
                index_stats,
                max(ceil(index_stats.row_count/3), 1) if self.is_range else 1)
        else:
            if self.is_by_primary_key():
                index_stats = self.context.zm.base_table_stats(self.context, self.meta)
            else:
                index_stats = self.context.zm.secondary_index_stats(self.context, self.meta, self.key_name)
            column_type = self.compiled.output_metadata.column_types[0]
            column = valexpr.leaf.RelativeColumnRef(0, 0, column_type)
            val = valexpr.leaf.Literal.from_any(column_type.dummy_value, column_type)
            cond = valexpr.binary.GT(column, val) if self.is_range else valexpr.binary.EQ(column, val)
            new_stats = self.context.zm.selection_stats(index_stats, cond)
        self_reads = new_stats.block_count()
        if index_stats.tree_height is not None and index_stats.tree_height > 1:
            self_reads += index_stats.tree_height - 1
        return QPop.EstimatedProps(
            stats = new_stats,
            blocks = QPop.StatsInBlocks(
                self_reads = self_reads,
                self_writes = 0,
                overall = self_reads))

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        f: BplusTree|HeapFile
        if self.key_name == INTERNAL_ROW_ID_COLUMN_NAME:
            f = self.context.mm.table_storage(self.context.tx, self.meta)
        else:
            column_index = self.meta.column_names.index(self.key_name)
            f = self.context.mm.index_storage(self.context.tx, self.meta, column_index)
        with f as file:
            if isinstance(file, BplusTree):
                if self.key_lower == self.key_upper and self.key_lower is not None:
                    for key, row in file.iter_get(self.key_lower):
                        yield (key, *row)
                else:
                    with closing(file.iter_scan(self.key_lower)) as iter:
                        for key, row in iter:
                            if self.key_lower is not None and self.lower_exclusive and key <= self.key_lower:
                                continue
                            elif self.key_upper is not None and (key > self.key_upper or (self.upper_exclusive and key >= self.key_upper)):
                                break
                            yield (key, *row)
            else:
                if self.key_lower == self.key_upper and self.key_lower is not None:
                    row = file.get(self.key_lower)
                    if row is not None:
                        yield row
                else:
                    raise ExecutorException('unexpected error')
        return
