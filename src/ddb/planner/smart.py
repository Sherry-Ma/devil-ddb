# Cost-based, dynamic programming
from typing import cast, Final

from ..globals import DEFAULT_SORT_BUFFER_SIZE, DEFAULT_SORT_LAST_BUFFER_SIZE, DEFAULT_BNLJ_BUFFER_SIZE, DEFAULT_HASH_BUFFER_SIZE
from ..validator import valexpr, ValExpr, SFWGHLop, BaseTableLop
from ..executor import StatementContext, QPop, TableScanPop, BNLJoinPop, FilterPop, ProjectPop, IndexScanPop, IndexNLJoinPop, MergeEqJoinPop, MergeSortPop, HashEqJoinPop

from .interface import Planner, PlannerException
from .util import add_groupby_by_sorting, add_having_and_select



# ======== Milestone 4: Smart Planner ========
class SmartPlanner(Planner):
    @classmethod
    def optimize_block(cls, context: StatementContext, block: SFWGHLop) -> QPop:
        # Implement your code here; optimize_block should return 
        # the optimized query plan found by your smart optimizer
        raise NotImplementedError
