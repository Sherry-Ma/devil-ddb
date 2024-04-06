from abc import abstractmethod

from ...types import ValType

from .interface import ValExpr, ValidatorException, ArithOpValExpr
from .func import FunCallValExpr

class AggrValExpr(FunCallValExpr):
    """An abstract class for a built-in aggregation function.
    """

    def __init__(self, args: tuple[ValExpr, ...], is_distinct: bool = False):
        """Constructor.
        ``is_distinct`` indicates the presence of DISTINCT within aggregate function.
        """
        super().__init__(args)
        self.is_distinct = is_distinct
        return

    def is_incremental(self) -> bool:
        """Determine whether this aggregation function can be incrementally computed
        give each input value one at a time, using only constant-space state.
        By default, we assume DISTINCT implies that the aggregate is not incremental;
        otherwise, the aggregate is incremental.
        This is obviously not true in general, so subclass should override this method as appropriate.
        """
        return not self.is_distinct

    def to_str(self) -> str:
        return '{}({}{})'.format(
            type(self).name,
            'DISTINCT ' if self.is_distinct else '',
            ', '.join(c.to_str() for c in self.children()))

    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        # this method is disabled because aggregates are special!
        raise TypeError

    @abstractmethod
    def code_str_init(self) -> str:
        """Generate a Python expression for computing, inside :class:`.AggrPop`,
        the initial aggregate state for a group (when no input has been seen yet).
        """
        pass

    @abstractmethod
    def code_str_add(self, state: str, child_code_str: str) -> str:
        """Generate a Python expression for updating, inside :class:`.AggrPop`,
        the aggregate state for a group given a new input value.
        ``child_code_str`` is the Python expression for the input to this aggregate function;
        when evaluated over each row of the group, it provides a new value to be incorporated into the aggregate state.
        """
        pass

    @abstractmethod
    def code_str_merge(self, state1: str, state2: str) -> str:
        """Generate a Python expression for merging, inside :class:`.AggrPop`,
        the aggregate states computed over two disjoint subsets of rows in a group.
        """
        pass

    @abstractmethod
    def code_str_finalize(self, state: str) -> str:
        """Generate a Python expression for computing, inside :class:`.AggrPop`,
        the final aggregate value for a group from the aggregate state (after seeing all input values).
        """
        pass

class SUM(ArithOpValExpr, AggrValExpr):
    name = 'SUM'
    arity_min = 1
    arity_max = 1

    def code_str_init(self) -> str:
        return '0' if self.valtype() == ValType.INTEGER else '0.0'

    def code_str_add(self, state: str, child_code_str: str) -> str:
        return f'({state})+({child_code_str})'

    def code_str_merge(self, state1: str, state2: str) -> str:
        return f'({state1})+({state2})'

    def code_str_finalize(self, state: str) -> str:
        return f'{state}'

class COUNT(AggrValExpr):
    name = 'COUNT'
    arity_min = 1
    arity_max = 1

    def _validate_valtype(self) -> tuple[ValType, ...]:
        return ValType.INTEGER, self.children()[0].valtype()

    def code_str_init(self) -> str:
        return '0'

    def code_str_add(self, state: str, child_code_str: str) -> str:
        return f'({state})+1'

    def code_str_merge(self, state1: str, state2: str) -> str:
        return f'({state1})+({state2})'

    def code_str_finalize(self, state: str) -> str:
        return f'{state}'

class AVG(AggrValExpr):
    name = 'AVG'
    arity_min = 1
    arity_max = 1

    def _validate_valtype(self) -> tuple[ValType, ...]:
        child_type = self.children()[0].valtype()
        if child_type not in (ValType.INTEGER, ValType.FLOAT):
            raise ValidatorException(f'operand of {type(self).__name__} is not numeric')
        return ValType.FLOAT, child_type

    def code_str_init(self) -> str:
        return '(0.0, 0)'

    def code_str_add(self, state: str, child_code_str: str) -> str:
        return f'(({state})[0] + {child_code_str}, ({state})[1] + 1)'

    def code_str_merge(self, state1: str, state2: str) -> str:
        return f'(({state1})[0] + ({state2})[0], ({state1})[1] + ({state2})[1])'

    def code_str_finalize(self, state: str) -> str:
        return f'None if ({state})[1] == 0 else ({state})[0] / float(({state})[1])'

class STDDEV_POP(AggrValExpr):
    name = 'STDDEV_POP'
    arity_min = 1
    arity_max = 1

    def _validate_valtype(self) -> tuple[ValType, ...]:
        child_type = self.children()[0].valtype()
        if child_type not in (ValType.INTEGER, ValType.FLOAT):
            raise ValidatorException(f'operand of {type(self).__name__} is not numeric')
        return ValType.FLOAT, child_type

    def code_str_init(self) -> str:
        # sum of squares, sum of values, count:
        return '(0.0, 0.0, 0)'

    def code_str_add(self, state: str, child_code_str: str) -> str:
        return f'(({state})[0] + ({child_code_str})*({child_code_str}), ({state})[1] + ({child_code_str}), ({state})[2] + 1)'

    def code_str_merge(self, state1: str, state2: str) -> str:
        return f'(({state1})[0] + ({state2})[0], ({state1})[1] + ({state2})[1], ({state1})[2] + ({state2})[2])'

    def code_str_finalize(self, state: str) -> str:
        return f'None if ({state})[2] == 0 else sqrt(({state})[0] / float(({state})[2]) - (({state})[1] / float(({state})[2])) * (({state})[1] / float(({state})[2])))'

class MIN(AggrValExpr):
    name = 'MIN'
    arity_min = 1
    arity_max = 1

    def _validate_valtype(self) -> tuple[ValType, ...]:
        child_type = self.children()[0].valtype()
        return child_type, child_type

    def is_incremental(self) -> bool:
        return True # even if DISTINCT

    def code_str_init(self) -> str:
        return 'None'
    
    def code_str_add(self, state: str, child_code_str: str) -> str:
        return f'{child_code_str} if {state} is None else min({state},{child_code_str})'

    def code_str_merge(self, state1: str, state2: str) -> str:
        return f'{state2} if {state1} is None else ({state1} if {state2} is None else min({state1},{state2}))'

    def code_str_finalize(self, state: str) -> str:
        return f'{state}'

class MAX(AggrValExpr):
    name = 'MAX'
    arity_min = 1
    arity_max = 1

    def _validate_valtype(self) -> tuple[ValType, ...]:
        child_type = self.children()[0].valtype()
        return child_type, child_type

    def is_incremental(self) -> bool:
        return True # even if DISTINCT

    def code_str_init(self) -> str:
        return 'None'
    
    def code_str_add(self, state: str, child_code_str: str) -> str:
        return f'{child_code_str} if {state} is None else max({state},{child_code_str})'

    def code_str_merge(self, state1: str, state2: str) -> str:
        return f'{state2} if {state1} is None else ({state1} if {state2} is None else max({state1},{state2}))'

    def code_str_finalize(self, state: str) -> str:
        return f'{state}'
