from typing import final, Iterable
from abc import ABC, abstractmethod

from ..globals import ANSI
from ..metadata import TableMetadata

class ValidatorException(Exception):
    pass

class Lop(ABC):
    """A logical representation of a SQL statement.
    """

    @abstractmethod
    def is_read_only(self) -> bool:
        """Whether this statement is ready-only (or may write the database).
        """
        pass

    @abstractmethod
    def modifies_schema(self) -> bool:
        """Whether this statement modifies the database schema.
        """
        pass

    def pstr_more(self) -> Iterable[str]:
        """Pretty-print additional information not already covered by :meth:`.Lop.pstr`.
        Subclasses should override this method as needed.
        """
        yield from ()
        return

    @final
    def pstr(self, indent: int = 0) -> Iterable[str]:
        """Produce a sequence of lines for pretty-printing the object.
        """
        prefix = '' if indent == 0 else '    ' * (indent-1) + '\\___'
        yield f'{prefix}{ANSI.EMPH}{type(self).__name__}{ANSI.END}[{hex(id(self))}]'
        prefix = '    ' * indent + '| '
        for s in self.pstr_more():
            yield f'{prefix}{s}'
        return

class QLop(Lop):
    """A logical representation of a query statement or fragment
    that returns a table as its output.
    """
    @final
    def is_read_only(self) -> bool:
        return True

    @final
    def modifies_schema(self) -> bool:
        return False

    @abstractmethod
    def metadata(self) -> TableMetadata:
        """Return a :class:`.TableMetadata` object describing the output schema for this operator.
        """
        pass