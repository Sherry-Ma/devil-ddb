from typing import cast, Final, Any

from ...types import ValType

from ..interface import ValidatorException
from .interface import ValExpr, UniTypeOpValExpr

class FunCallValExpr(ValExpr):
    """An abstract class for a built-in function call operator.
    """

    name: str | None = None
    """The function name to be used by default for generating Python code and display.
    Subclass must override it.
    NOTE: This should be best enforced as an abstract class attribute, but Python doesn't quite support it.
    """

    arity_min: int = 0
    """The minimum number of arguments expected.
    Subclass must override it.
    NOTE: This should be best enforced as an abstract class attribute, but Python doesn't quite support it.
    """

    arity_max: int | None = None
    """The maximum number of arguments expected, or ``None`` if there is no cap.
    Subclass must override it.
    NOTE: This should be best enforced as an abstract class attribute, but Python doesn't quite support it.
    """

    def __init__(self, args: tuple[ValExpr, ...]) -> None:
        if type(self).arity_min is None:
            raise ValidatorException('unexpected error')
        if len(args) < type(self).arity_min:
            raise ValidatorException(f'fewer than {type(self).arity_min} arguments for {type(self).name}')
        if type(self).arity_max is not None and len(args) > cast(int, type(self).arity_max):
            raise ValidatorException(f'more than {type(self).arity_max} arguments for {type(self).name}')
        super().__init__(args)
        return

    def copy_with_new_children(self, new_children: tuple[ValExpr, ...]) -> ValExpr:
        return type(self)(new_children)

    def is_op_equivalent(self, other: ValExpr) -> bool:
        return isinstance(other, FunCallValExpr) and\
            type(self) == type(other) and\
            self._valtype == other._valtype

    def to_str(self) -> str:
        return '{}({})'.format(
            type(self).name,
            ', '.join(c.to_str() for c in self.children()))

    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        return '{}({})'.format(type(self).name, ', '.join(children_code_str))

class LOWER(UniTypeOpValExpr, FunCallValExpr):
    uni_type = ValType.VARCHAR
    name = 'LOWER'
    arity_min = 1
    arity_max = 1

    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        return f'({children_code_str[0]}).lower()'

class UPPER(UniTypeOpValExpr, FunCallValExpr):
    uni_type = ValType.VARCHAR
    name = 'UPPER'
    arity_min = 1
    arity_max = 1

    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        return f'({children_code_str[0]}).upper()'

class REPLACE(UniTypeOpValExpr, FunCallValExpr):
    uni_type = ValType.VARCHAR
    name = 'REPLACE'
    arity_min = 3
    arity_max = 3

    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        return f'({children_code_str[0]}).replace({children_code_str[1]}, {children_code_str[2]})'

class OFunCallValExpr(FunCallValExpr):
    """An abstract class for a built-in function call that accepts additional options,
    e.g., ``CAST(arg AS INTEGER)``, where ``INTEGER`` is an option.
    """

    OPTIONS: dict[str, set] = {}
    """The available options, as a dictionary that
    maps each available option name to a container of all possible values for this option.
    Subclass must override it.
    TODO: This should be best enforced as an abstract class attribute, but Python doesn't quite support it.
    """

    def __init__(self, args: tuple[ValExpr, ...], options: dict[str, Any]) -> None:
        for option, value in options.items():
            if option not in type(self).OPTIONS:
                raise ValidatorException(f'{option} is not a recognized option for {type(self).name}')
            if value not in type(self).OPTIONS[option]:
                raise ValidatorException(
                    f'value for option {option} must be one of ' +\
                    ', '.join(str(v) for v in type(self).OPTIONS[option]))
        self.options: Final = options # set before super().__init__ because this is needed for type-checking
        super().__init__(args)
        return

    def copy_with_new_children(self, new_children: tuple[ValExpr, ...]) -> ValExpr:
        return type(self)(new_children, self.options.copy())

    def is_op_equivalent(self, other: ValExpr) -> bool:
        return isinstance(other, OFunCallValExpr) and\
            super().is_op_equivalent(other) and\
            self.options == other.options # default equality should work unless the value options are opaque objects

    def to_str(self) -> str:
        return '{}[{}]({})'.format(
            type(self).name,
            ', '.join(f'{k}: {v}' for k, v in self.options.items()),
            ', '.join(c.to_str() for c in self.children()))

class CAST(OFunCallValExpr):
    name = 'CAST'
    arity_min = 1
    arity_max = 1
    OPTIONS = {
        'AS': { ValType.INTEGER, ValType.BOOLEAN, ValType.DATETIME, ValType.FLOAT, ValType.VARCHAR }
    }

    def _validate_valtype(self) -> tuple[ValType, ...]:
        arg = self.children()[0]
        if not arg.valtype().can_cast_to(self.options['AS']):
            raise ValidatorException('cannot CAST {} to {}'.format(arg.valtype(), self.options['AS']))
        return (self.options['AS'], arg.valtype())

    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        arg = self.children()[0]
        if arg.valtype() == self.options['AS']:
            return children_code_str[0]
        elif arg.valtype() == ValType.DATETIME and self.options['AS'] == ValType.VARCHAR:
            return f'({children_code_str[0]}).isoformat()'
        elif arg.valtype() == ValType.VARCHAR and self.options['AS'] == ValType.DATETIME:
            return f'str_to_datetime({children_code_str[0]})'
        elif self.options['AS'] == ValType.VARCHAR:
            return f'str({children_code_str[0]})'
        elif self.options['AS'] == ValType.FLOAT:
            return f'float({children_code_str[0]})'
        elif self.options['AS'] == ValType.INTEGER:
            return f'int({children_code_str[0]})'
        else:
            raise ValidatorException('unexpected error')
