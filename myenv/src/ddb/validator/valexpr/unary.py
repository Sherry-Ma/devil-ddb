from ..interface import ValidatorException
from .interface import ValExpr, ArithOpValExpr, BoolOpValExpr

class UnaryOpValExpr(ValExpr):
    """An abstract class for a unary operator.
    """

    op: str | None = None
    """The op symbol to be used by default for generating Python code and display.
    Subclass must override it.
    NOTE: This should be best enforced as an abstract class attribute, but Python doesn't quite support it.
    """

    def __init__(self, input: ValExpr) -> None:
        super().__init__((input, ))
        return

    def copy_with_new_children(self, new_children: tuple[ValExpr, ...]) -> ValExpr:
        if len(new_children) != 1:
            raise ValidatorException(f'one input expected by {type(self).__name__}')
        return type(self)(new_children[0])

    def is_op_equivalent(self, other: ValExpr) -> bool:
        return isinstance(other, UnaryOpValExpr) and\
            type(self) == type(other) and\
            self.valtype() == other.valtype()

    def to_str(self) -> str:
        return f'{type(self).op}({self.children()[0].to_str()})'

    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        input_code = children_code_str[0]
        return f'{type(self).op}({input_code})'

class NEG(ArithOpValExpr, UnaryOpValExpr):
    op = '-'

class NOT(BoolOpValExpr, UnaryOpValExpr):
    op = 'not'
