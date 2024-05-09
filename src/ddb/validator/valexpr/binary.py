from ...types import ValType

from ..interface import ValidatorException
from .interface import ValExpr, ArithOpValExpr, UniTypeOpValExpr, BoolOpValExpr

class BinaryOpValExpr(ValExpr):
    """An abstract class for a binary operator.
    """

    op: str | None = None
    """The op symbol to be used by default for generating Python code and display.
    Subclass must override it.
    NOTE: This should be best enforced as an abstract class attribute, but Python doesn't quite support it.
    """

    def __init__(self, left: ValExpr, right: ValExpr) -> None:
        super().__init__((left, right))
        return

    def copy_with_new_children(self, new_children: tuple[ValExpr, ...]) -> ValExpr:
        if len(new_children) != 2:
            raise ValidatorException(f'two inputs expected by {type(self).__name__}')
        left, right = new_children
        return type(self)(left, right)

    def is_op_equivalent(self, other: ValExpr) -> bool:
        return isinstance(other, BinaryOpValExpr) and\
            type(self) == type(other) and\
            self._valtype == other._valtype

    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        left_code, right_code = children_code_str
        return f'({left_code}) {type(self).op} ({right_code})'

    def to_str(self) -> str:
        return f'({self.children()[0].to_str()}) {type(self).op} ({self.children()[1].to_str()})'

    def left(self) -> ValExpr:
        return self.children()[0]

    def right(self) -> ValExpr:
        return self.children()[1]

class PLUS(ArithOpValExpr, BinaryOpValExpr):
    op = '+'

class MINUS(ArithOpValExpr, BinaryOpValExpr):
    op = '-'

class MULTIPLY(ArithOpValExpr, BinaryOpValExpr):
    op = '*'

class DIVIDE(ArithOpValExpr, BinaryOpValExpr):
    op = '/'

    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        left_code, right_code = children_code_str
        if self.valtype() == ValType.INTEGER:
            # when Python divides two ints by /, it's actually a float division;
            # use // to force integer division:
            return f'({left_code}) // ({right_code})'
        else:
            return super()._code_str(children_code_str)

class MOD(UniTypeOpValExpr, BinaryOpValExpr):
    uni_type = ValType.INTEGER
    op = '%'

class CONCAT(UniTypeOpValExpr, BinaryOpValExpr):
    uni_type = ValType.VARCHAR
    op = '+'

class REGEXPLIKE(UniTypeOpValExpr, BinaryOpValExpr):
    uni_type = ValType.VARCHAR
    op = '~'

    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        left_code, right_code = children_code_str
        return f'regexp_match({left_code}, {right_code})'

class AND(BoolOpValExpr, BinaryOpValExpr):
    op = 'and'

class OR(BoolOpValExpr, BinaryOpValExpr):
    op = 'or'

class CompareOpValExpr(BinaryOpValExpr):
    def _validate_valtype(self) -> tuple[ValType, ...]:
        left, right = self.children()
        if left.valtype() == right.valtype():
            desired_child_type = left.valtype()
        elif right.valtype().implicitly_casts_to(left.valtype()):
            desired_child_type = left.valtype()
        elif left.valtype().implicitly_casts_to(right.valtype()):
            desired_child_type = right.valtype()
        else:
            raise ValidatorException(f'cannot directly compare {left.valtype().name} and {right.valtype().name}')
        return ValType.BOOLEAN, desired_child_type, desired_child_type

class EQ(CompareOpValExpr):
    op = '=='

class NE(CompareOpValExpr):
    op = '!='

class LT(CompareOpValExpr):
    op = '<'

class LE(CompareOpValExpr):
    op = '<='

class GT(CompareOpValExpr):
    op = '>'

class GE(CompareOpValExpr):
    op = '>='
