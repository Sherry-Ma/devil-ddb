from typing import cast, final, TYPE_CHECKING
from abc import ABC, abstractmethod

from ...util import CustomInitMeta
from ...types import ValType

from ..interface import ValidatorException

# NOTE: this module depends on .util.cast_as_needed but importing it here causes circular imports.
# currently it's imported only when needed; search in code below.

class ValExpr(ABC, metaclass=CustomInitMeta):
    """An expression tree that evaluates to an atomic value (as opposed to a collection),
    or, more precisely, a expression tree node representing a subtree that evaluates to an atomic value.
    """
    @abstractmethod
    def __init__(self, children: tuple['ValExpr', ...]) -> None:
        self._children = children
        self._valtype: ValType | None = None
        return

    @final
    def __post_init__(self) -> None:
        # perform type-checking and automatically add casts as needed:
        self._valtype, *desired_child_type_list = self._validate_valtype()
        new_child_list = list()
        if len(self.children()) != len(desired_child_type_list):
            raise ValidatorException('unexpected error')
        for child, desired_type in zip(self.children(), desired_child_type_list):
            if child.valtype().implicitly_casts_to(desired_type):
                from .util import cast_if_needed
                new_child_list.append(cast_if_needed(child, desired_type))
            else:
                raise ValidatorException('unexpected error')
        self._children = tuple(new_child_list)
        return

    @final
    def children(self) -> tuple['ValExpr', ...]:
        """Return this expression's child expressions.
        """
        return self._children

    @final
    def valtype(self) -> ValType:
        """Return this expression's inferred type.
        """
        if self._valtype is None:
            raise ValidatorException('unexpected error')
        return self._valtype

    @abstractmethod
    def _validate_valtype(self) -> tuple[ValType, ...]:
        """Validate input types and return the output type,
        along with what these input types should first be casted to before the actual operation.
        """

    @abstractmethod
    def copy_with_new_children(self, new_children: tuple['ValExpr', ...]) -> 'ValExpr':
        """Make a copy of this node, but with the new children given.
        Return the new node.
        """
        pass

    @abstractmethod
    def is_op_equivalent(self, other: 'ValExpr') -> bool:
        """Check if the results produced by this expression and ``other`` are always equivalent
        (not just by equality but also by type), assuming that both are given identifical children.
        """
        pass

    @abstractmethod
    def to_str(self) -> str:
        """Convert to a one-liner for viewing.
        """
        pass

    @abstractmethod
    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        """Convert to a Python expression for evaluation inside a :class:`.QPop`.
        ``children_code_str`` are Python expressions for this expression's children (if any).
        This method is intended to be a helper for :func:`.to_code_str`.
        :class:`.NamedColumnRef` and :class:`.RelativeColumnRef` override this method
        since they require additional information for this conversion.
        """
        pass

class ArithOpValExpr(ValExpr):
    """An arithmetic operator.
    """
    def _validate_valtype(self) -> tuple[ValType, ...]:
        result_valtype = ValType.INTEGER # default
        for i, child_type in enumerate(child.valtype() for child in self.children()):
            if child_type not in (ValType.INTEGER, ValType.FLOAT):
                raise ValidatorException(f'{i}-th operand of {type(self).__name__} is not numeric')
            if child_type == ValType.FLOAT:
                result_valtype = ValType.FLOAT # upgrade
        return result_valtype, *([result_valtype] * len(self.children())) # make all input types match

class UniTypeOpValExpr(ValExpr):
    """An operators that expects all inputs and output to be of same exact type.
    """

    uni_type: ValType | None = None
    """The type to be used by all inputs and output.
    Subclass must override it.
    NOTE: This should be best enforced as an abstract class attribute, but Python doesn't quite support it.
    """

    def _validate_valtype(self) -> tuple[ValType, ...]:
        if type(self).uni_type is None:
            raise ValidatorException('unexpect error')
        target_type = cast(ValType, type(self).uni_type)
        for i, child_type in enumerate(child.valtype() for child in self.children()):
            if child_type != target_type:
                raise ValidatorException(f'{i}-th operand of {type(self).__name__} is not {target_type.name})')
        return target_type, *([target_type] * len(self.children()))

class BoolOpValExpr(UniTypeOpValExpr):
    """A Boolean operator.
    """
    uni_type = ValType.BOOLEAN
