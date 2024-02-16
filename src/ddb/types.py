from typing import Any, TypeAlias
from enum import Enum, auto
from functools import cached_property
from sys import getsizeof
from datetime import datetime
from dateutil.parser import parse as dateutil_parse

class ValType(Enum):
    """Types supported by our database system.
    Conveniently, the names are also valid SQL types (except for ``ANY``).
    Note that the ordering reflects type precedence:
    when an operator combines expressions of different data types,
    the expression whose data type has lower precedence is first converted to one with the higher precedence
    (assuming an implicit cast is possible).
    """
    DATETIME = auto()
    FLOAT = auto()
    INTEGER = auto()
    BOOLEAN = auto()
    VARCHAR = auto()
    ANY = auto()

    def implicitly_casts_to(self, other: 'ValType') -> bool:
        """Check if a value of this type can be implicitly cast to a value of the ``other`` type.
        """
        if self == other:
            return True
        elif self == ValType.BOOLEAN and other in (ValType.INTEGER, ValType.FLOAT):
            return True
        elif self == ValType.INTEGER and other == ValType.FLOAT:
            return True
        elif self == ValType.VARCHAR and other == ValType.DATETIME:
            return True
        elif self == ValType.DATETIME and other == ValType.VARCHAR:
            return True
        elif other == ValType.ANY:
            return True
        else:
            return False

    def can_cast_to(self, other: 'ValType') -> bool:
        """Check if a value of this type can be explicitly cast to a value of the ``other`` type.
        """
        if self.implicitly_casts_to(other):
            return True
        elif self == ValType.ANY:
            return True
        elif other == ValType.VARCHAR:
            return True
        elif self == ValType.FLOAT and other == ValType.INTEGER:
            return True
        else:
            return False

    def cast_from(self, v: Any) -> Any:
        """Cast the given Python value into another Python value corresponding to this type.
        """
        match self:
            case ValType.DATETIME:
                return dateutil_parse(v)
            case ValType.FLOAT:
                return float(v)
            case ValType.INTEGER:
                return int(v)
            case ValType.BOOLEAN:
                return bool(v)
            case ValType.VARCHAR:
                return str(v)
            case ValType.ANY:
                return v

    @cached_property
    def size(self) -> int:
        """Size of an object of this type in bytes, in memory.
        In the case of variable-length types ``VARCHAR`` and ``ANY``, we can only return a random guess.
        NOTE: In DDB, the disk representation of the object may take a different number of bytes
        (often fewer because Python isn't very efficient with basic types).
        """
        return getsizeof(self.dummy_value)

    @cached_property
    def dummy_value(self) -> Any:
        """A dummy Python value for this type.
        """
        match self:
            case ValType.DATETIME:
                return datetime.now()
            case ValType.FLOAT:
                return float(0.142857)
            case ValType.INTEGER:
                return int(142857)
            case ValType.BOOLEAN:
                return False
            case ValType.VARCHAR:
                return '{:_^128}'.format('''DDB is Devil's DataBase, an instructional database system developed at Duke.''')
            case ValType.ANY:
                return '{:*^128}'.format('''DDB is Devil's DataBase, an instructional database system developed at Duke.''')

RowType: TypeAlias = list[ValType]
"""Type for a row, which is simply a list of ``ValType``s.
"""

def column_sizes(row_type: RowType) -> list[int]:
    """Return the sizes of columns according to their types (:meth:`.ValType.size`).
    """
    return [t.size for t in row_type]

def row_size(row_type: RowType) -> int:
    """Return the size the row according to the column types (:meth:`.ValType.size`).
    """
    return sum(column_sizes(row_type))
