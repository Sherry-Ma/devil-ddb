"""This package implements the physical plan operators that execute commands and queries.
During execution, each physical query plan operator (:class:`.QPop`) uses a Python generator
to consume its inputs and produce its output in a streaming fashion, one row at time.

Delegation of Buffering
^^^^^^^^^^^^^^^^^^^^^^^

By convention, each operator should only buffer input/output rows in memory
if doing so is beneficial to itself.
This convention ensures that we don't buffer unnecessarily,
because we cannot anticipate how other operators in a query plan produce outputs and consume inputs.

For example, let's apply this convention to :class:`.BNLJoinPop`.
What it needs is to buffer its left input in order to reduce the number of passes over its right input.
However, this operator won't buffer its output because its only obligation is to stream that to its parent:
if the parent will buffer that if doing so is beneficial to the parent
(e.g., a :class:`.MaterializePop` may do so before writing that to disk).
Also, the :class:`.BNLJoinPop` operator only needs to stream its right input in one row at a time:
if the right input happens to originate from a file scan,
some operator in the right subtree would have buffered rows from the disk file a block at a time.
"""

from .interface import ExecutorException, Pop, QPop, CPop, StatementContext
from .command import CreateTablePop, ShowTablesPop, AnalyzeStatsPop, CreateIndexPop, InsertPop, DeletePop
from .literaltable import LiteralTablePop
from .tablescan import TableScanPop
from .filter import FilterPop
from .project import ProjectPop
from .indexscan import IndexScanPop
from .mergesort import MergeSortPop
from .materialize import MaterializePop
from .join.bnlj import BNLJoinPop
from .join.mergeeqj import MergeEqJoinPop
from .join.indexnlj import IndexNLJoinPop
from .join.hasheqj import HashEqJoinPop
