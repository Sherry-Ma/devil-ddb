from typing import Final
from abc import abstractmethod

from ..interface import QPop, P

class JoinPop(QPop[P]):
    @abstractmethod
    def __init__(self, left: QPop[QPop.CompiledProps], right: QPop[QPop.CompiledProps]):
        super().__init__(left.context)
        self.left: Final = left
        self.right: Final = right
        return

    def children(self) -> tuple[QPop[QPop.CompiledProps], ...]:
        return (self.left, self.right)
