from typing import Final, Iterable

from ..globals import ANSI
from ..metadata import TableMetadata, BaseTableMetadata, INTERNAL_ROW_ID_COLUMN_NAME, INTERNAL_ROW_ID_COLUMN_TYPE

from .interface import Lop, QLop
from .valexpr import ValExpr

class BaseTableLop(QLop):
    def __init__(self, base_metadata: BaseTableMetadata, return_row_id: bool = False) -> None:
        self.base_metadata: Final = base_metadata
        self.return_row_id: bool = return_row_id
        self.inferred_metadata = TableMetadata([INTERNAL_ROW_ID_COLUMN_NAME] + self.base_metadata.column_names,
                                               [INTERNAL_ROW_ID_COLUMN_TYPE] + self.base_metadata.column_types) \
            if self.return_row_id else self.base_metadata
        return

    def metadata(self) -> TableMetadata:
        return self.inferred_metadata

    def pstr_more(self) -> Iterable[str]:
        yield from self.base_metadata.pstr()
        if self.return_row_id:
            yield 'also expected to return internal row id'
        return

class LiteralTableLop(QLop):
    def __init__(self, inferred_metadata: TableMetadata, rows: list[tuple]) -> None:
        """We assume here that all ``rows`` already confirm to ``inferred_metadata``.
        """
        self.inferred_metadata: Final = inferred_metadata
        self.rows: Final = rows
        return

    def metadata(self) -> TableMetadata:
        return self.inferred_metadata

    def pstr_more(self) -> Iterable[str]:
        yield from self.inferred_metadata.pstr()
        for row in self.rows:
            yield str(row)
        return

class SFWGHLop(QLop):
    def __init__(self,
                 select_valexprs: list[ValExpr],
                 select_aliases: list[str],
                 from_tables: list[QLop],
                 from_aliases: list[str],
                 where_cond: ValExpr | None = None,
                 groupby_valexprs: list[ValExpr] | None = None,
                 having_cond: ValExpr | None = None) -> None:
        self.select_valexprs: Final = select_valexprs
        self.select_aliases: Final = select_aliases
        self.from_tables: Final = from_tables
        self.from_aliases: Final = from_aliases
        self.where_cond: Final = where_cond
        self.groupby_valexprs: Final = groupby_valexprs
        self.having_cond: Final = having_cond
        self.inferred_metadata = TableMetadata(select_aliases, [e.valtype() for e in select_valexprs])
        return

    def metadata(self) -> TableMetadata:
        return self.inferred_metadata

    def pstr_more(self) -> Iterable[str]:
        yield f'{ANSI.H2}SELECT:{ANSI.END}'
        for select_valexpr, select_alias in zip(self.select_valexprs, self.select_aliases):
            yield f' {select_alias}: ' + select_valexpr.to_str()
        if self.where_cond is not None:
            yield f'{ANSI.H2}WHERE:{ANSI.END} ' + self.where_cond.to_str()
        if self.groupby_valexprs is not None:
            yield f'{ANSI.H2}GROUP BY:{ANSI.END}'
            for groupby_valexpr in self.groupby_valexprs:
                yield ' ' + groupby_valexpr.to_str()
        if self.having_cond is not None:
            yield f'{ANSI.H2}HAVING:{ANSI.END} ' + self.having_cond.to_str()
        yield f'{ANSI.H2}FROM:{ANSI.END} ' + ', '.join(self.from_aliases)
        for from_table, from_alias in zip(self.from_tables, self.from_aliases):
            for i, s in enumerate(from_table.pstr()):
                if i == 0:
                    yield f' {s} AS {from_alias}'
                else:
                    yield f' {s}'
        return

class CreateTableLop(Lop):
    def __init__(self, base_metadata: BaseTableMetadata) -> None:
        self.base_metadata: Final = base_metadata
        return

    def is_read_only(self) -> bool: return False

    def modifies_schema(self) -> bool: return True

    def pstr_more(self) -> Iterable[str]:
        yield from self.base_metadata.pstr()
        return

class ShowTablesLop(Lop):
    def __init__(self) -> None:
        return

    def is_read_only(self) -> bool: return True

    def modifies_schema(self) -> bool: return False

class AnalyzeStatsLop(Lop):
    def __init__(self, base_metas: list[BaseTableMetadata] | None = None) -> None:
        self.base_metas: Final = base_metas
        return

    def is_read_only(self) -> bool: return True

    def modifies_schema(self) -> bool: return False

    def pstr_more(self) -> Iterable[str]:
        if self.base_metas is not None:
            yield ', '.join(meta.name for meta in self.base_metas)
        return

class CreateIndexLop(Lop):
    def __init__(self, base_metadata: BaseTableMetadata, column_index: int) -> None:
        self.base_metadata: Final = base_metadata
        self.column_index: Final = column_index
        return

    def is_read_only(self) -> bool: return False

    def modifies_schema(self) -> bool: return True

    def pstr_more(self) -> Iterable[str]:
        yield from self.base_metadata.pstr()
        yield f'key column index: {self.column_index}'
        return

class DeleteLop(Lop):
    def __init__(self, base_metadata: BaseTableMetadata, key_query: SFWGHLop) -> None:
        self.base_metadata: Final = base_metadata
        self.key_query: Final = key_query
        return

    def is_read_only(self) -> bool: return False

    def modifies_schema(self) -> bool: return False

    def pstr_more(self) -> Iterable[str]:
        yield from self.base_metadata.pstr()
        for s in self.key_query.pstr():
            yield ' ' + s
        return

class InsertLop(Lop):
    def __init__(self, base_metadata: BaseTableMetadata, contents: QLop) -> None:
        """We assume here that ``contents`` are can be implicitly cast 
        """
        self.base_metadata: Final = base_metadata
        self.contents: Final = contents
        return

    def is_read_only(self) -> bool: return False

    def modifies_schema(self) -> bool: return False

    def pstr_more(self) -> Iterable[str]:
        yield from self.base_metadata.pstr()
        yield from self.contents.pstr()
        return

class SetOptionLop(Lop):
    def __init__(self, option: str, value: str) -> None:
        self.option: Final = option
        self.value: Final = value
        return

    def is_read_only(self) -> bool: return True

    def modifies_schema(self) -> bool: return False

    def pstr_more(self) -> Iterable[str]:
        yield f'{self.option} {self.value}'
        return

class CommitLop(Lop):
    def is_read_only(self) -> bool: return True
    def modifies_schema(self) -> bool: return False

class RollbackLop(Lop):
    def is_read_only(self) -> bool: return True
    def modifies_schema(self) -> bool: return False
