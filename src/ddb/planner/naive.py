from ..globals import DEFAULT_BNLJ_BUFFER_SIZE
from ..validator import SFWGHLop, BaseTableLop
from ..executor import StatementContext, QPop, TableScanPop, BNLJoinPop, FilterPop, ProjectPop

from .interface import Planner, PlannerException

class NaivePlanner(Planner):
    @classmethod
    def optimize_block(cls, context: StatementContext, block: SFWGHLop) -> QPop:
        plan: QPop | None = None
        for input_table, input_alias in zip(block.from_tables, block.from_aliases):
            if isinstance(input_table, BaseTableLop):
                table_scan = TableScanPop(context, input_alias, input_table.base_metadata)
            else:
                raise PlannerException('subqueries in FROM not supported')
            if plan is None:
                plan = table_scan
            else:
                plan = BNLJoinPop(plan, table_scan, None, DEFAULT_BNLJ_BUFFER_SIZE)
        if plan is None:
            raise PlannerException('unexpected error')
        if block.where_cond is not None:
            plan = FilterPop(plan, block.where_cond)
        plan = ProjectPop(plan, block.select_valexprs, block.select_aliases)
        return plan
