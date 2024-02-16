from typing import Final, Any
import copy

from ...types import ValType

from ..interface import ValidatorException
from .interface import ValExpr
from .util import OutputLineage, find_column_in_lineage

class LeafValExpr(ValExpr):
    """A leaf node in an expression tree with no children.
    """
    def copy_with_new_children(self, new_children: tuple[ValExpr, ...] = ()) -> ValExpr:
        return copy.deepcopy(self)

class Literal(LeafValExpr):
    """A literal (constant).
    """
    def __init__(self, value: Any) -> None:
        super().__init__(())
        self.value: Final = value
        return

    def is_op_equivalent(self, other: ValExpr) -> bool:
        return isinstance(other, Literal) and\
            type(self) == type(other) and\
            type(self.value) == type(self.value) and\
            self.value == other.value

    @classmethod
    def from_any(cls, val: Any, valtype: ValType) -> 'Literal':
        match valtype:
            case ValType.INTEGER:
                return LiteralNumber(int(val))
            case ValType.FLOAT:
                return LiteralNumber(float(val))
            case ValType.VARCHAR:
                return LiteralString(str(val))
            case ValType.BOOLEAN:
                return LiteralBoolean(bool(val))
            case _:
                raise NotImplemented

class LiteralString(Literal):
    """A literal string value.
    """
    def __init__(self, value: str) -> None:
        super().__init__(value)
        return

    def _validate_valtype(self) -> tuple[ValType, ...]:
        return (ValType.VARCHAR, )

    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        return f'{self.value!r}' # make sure it's properly quoted

    def to_str(self) -> str:
        return f'{self.value!r}' # make sure it's properly quoted

class LiteralNumber(Literal):
    """A literal numeric (integer or float) value.
    """
    def __init__(self, value: int|float) -> None:
        super().__init__(value)
        return

    @classmethod
    def from_str(cls, str_value: str) -> 'LiteralNumber':
        value: int|float
        try:
            value = int(str_value)
        except ValueError:
            try:
                value = float(str_value)
            except ValueError:
                raise ValidatorException(f'unable to parse value as a number: {str_value}')
        return LiteralNumber(value)

    def _validate_valtype(self) -> tuple[ValType, ...]:
        if type(self.value) == int:
            return (ValType.INTEGER, )
        else:
            return (ValType.FLOAT, )

    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        return f'{type(self.value).__name__}({str(self.value)})'

    def to_str(self) -> str:
        return f'{type(self.value).__name__}({str(self.value)})'

class LiteralBoolean(Literal):
    """A literal boolean value.
    """
    def __init__(self, value: bool) -> None:
        super().__init__(value)
        return

    def _validate_valtype(self) -> tuple[ValType, ...]:
        return (ValType.BOOLEAN, )

    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        return f'{self.value}'

    def to_str(self) -> str:
        return f'{self.value}'

class RelativeColumnRef(LeafValExpr):
    """Reference to a column, in the context of a :class:`.QPop`,
    produced by that ``QPop``'s input(s).
    This kind of references make it easier for the query optimizer to manipulate plans,
    including making a ``QPop`` produce additional columns without names to be used later.
    """
    def __init__(self, input_index: int, column_index: int, column_type: ValType) -> None:
        super().__init__(())
        self.input_index: Final = input_index
        self.column_index: Final = column_index
        self.column_type: Final = column_type
        return

    def _validate_valtype(self) -> tuple[ValType, ...]:
        return (self.column_type, )

    def is_op_equivalent(self, other: ValExpr) -> bool:
        return isinstance(other, RelativeColumnRef) and\
            self.input_index == other.input_index and\
            self.column_index == other.column_index and\
            self.column_type == other.column_type

    def _code_str(self, children_code_str: tuple[str, ...],
                 output_lineages: list[OutputLineage] | None = None, row_vars: list[str] | None = None) -> str:
        if row_vars is None:
            raise TypeError
        return f'{row_vars[self.input_index]}[{self.column_index}]'

    def to_str(self) -> str:
        return f'((input){self.input_index}.(column){self.column_index})'

class NamedColumnRef(LeafValExpr):
    """A qualified reference to a column, consisting of both a table alias and a column name.
    """
    def __init__(self, table_alias: str, column_name: str, column_type: ValType) -> None:
        super().__init__(())
        self.table_alias: Final = table_alias
        self.column_name: Final = column_name
        self.column_type: Final = column_type
        return

    def _validate_valtype(self) -> tuple[ValType, ...]:
        return (self.column_type, )

    def is_op_equivalent(self, other: ValExpr) -> bool:
        return isinstance(other, NamedColumnRef) and\
            self.table_alias == other.table_alias and\
            self.column_name == other.column_name and\
            self.column_type == other.column_type

    def _code_str(self, children_code_str: tuple[str, ...],
                 output_lineages: list[OutputLineage] | None = None, row_vars: list[str] | None = None) -> str:
        if output_lineages is None or row_vars is None:
            raise TypeError
        for input_index, output_lineage in enumerate(output_lineages):
            if (column_index := find_column_in_lineage(self.table_alias, self.column_name, output_lineage)) is not None:
                return f'{row_vars[input_index]}[{column_index}]'
        raise ValidatorException(f'invalid column reference {self.table_alias}.{self.column_name}')

    def to_str(self) -> str:
        return f'{self.table_alias}.{self.column_name}'
