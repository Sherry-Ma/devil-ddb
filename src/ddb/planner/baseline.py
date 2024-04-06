from typing import cast, Final

from ..globals import DEFAULT_SORT_BUFFER_SIZE, DEFAULT_SORT_LAST_BUFFER_SIZE, DEFAULT_BNLJ_BUFFER_SIZE, DEFAULT_HASH_BUFFER_SIZE
from ..validator import valexpr, ValExpr, SFWGHLop, BaseTableLop
from ..executor import StatementContext, QPop, TableScanPop, BNLJoinPop, FilterPop, ProjectPop, IndexScanPop, IndexNLJoinPop, MergeEqJoinPop, MergeSortPop, HashEqJoinPop

from .interface import Planner, PlannerException
from .util import add_groupby_by_sorting, add_having_and_select

class BaselinePlanner(Planner):
    @classmethod
    def eqj_cond(cls, outer_table_aliases: list[str],
                 inner_table_alias: str, inner_table: BaseTableLop,
                 cond: ValExpr) \
        -> tuple[list[ValExpr], list[ValExpr], ValExpr | None] | None:
        """Consider an inner base table with alias ``inner_table_alias``,
        which is joined with a collection of outer tables with ``outer_table_aliases``,
        or is by itself (if ``outer_table_aliases`` is empty).
        Given condition ``cond``, find a good equality join condition with the base table.
        Return a triple consisting of
        1) a list of expressions (most likely columns) over the left input,
        2) a list of expressions (most likely columns) over the right input, to be equated to the above, and
        3) a "remainder" condition (or ``None`` if not needed) such that
        ANDing it with the equality condition is equivalent to the given condition.
        If nothing is identified, return ``None`` instead of a triple.

        We assume that ``cond`` can be evaluated over ``outer_table_aliases`` plus ``inner_table_alias``.
        """
        left_exprs: list[ValExpr] = list()
        right_exprs: list[ValExpr] = list()
        remaining_parts: list[ValExpr] = list()
        for part in valexpr.conjunctive_parts(cond):
            if isinstance(part, valexpr.binary.EQ) and \
                valexpr.in_scope(part.left(), outer_table_aliases) and valexpr.in_scope(part.right(), [inner_table_alias]):
                left_exprs.append(part.left())
                right_exprs.append(part.right())
            elif isinstance(part, valexpr.binary.EQ) and \
                valexpr.in_scope(part.left(), [inner_table_alias]) and valexpr.in_scope(part.right(), outer_table_aliases):
                right_exprs.append(part.left())
                left_exprs.append(part.right())
            else:
                remaining_parts.append(part)
        if len(left_exprs) > 0:
            return left_exprs, right_exprs,\
                (None if len(remaining_parts) == 0 else valexpr.make_conjunction(remaining_parts))
        else:
            return None

    @classmethod
    def ensure_sorted(cls, context: StatementContext, pop: QPop,
                      exprs: list[ValExpr], orders_asc_required: list[bool | None]) -> tuple[QPop, list[bool]]:
        """Ensure that ``pop`` is sorted according to ``exprs`` and ``orders_asc_required``
        by putting a :class:`.MergeSortPop` on top of ``pop`` if needed.
        An entry in ``orders_asc_required`` can be ``None`` to indicate that
        the corresponding expression can be either ascending or descending.
        Return the resulting plan along with the actual ordering selected.
        """
        props = pop.compiled
        sort_needed = False
        if len(props.ordered_columns) < len(exprs):
            sort_needed = True
        else:
            orders_asc: list[bool] = list()
            for expr, asc, col_i, col_asc in zip(exprs, orders_asc_required,
                                                 props.ordered_columns, props.ordered_asc):
                if (asc is not None and asc != col_asc) or\
                    (expr_col_i := pop.column_in_output(expr)) is None or\
                    expr_col_i != col_i:
                    sort_needed = True
                    break
                orders_asc.append(col_asc)
            if not sort_needed:
                return pop, orders_asc
        orders_asc = [ (asc if asc is not None else True) for asc in orders_asc_required ]
        return MergeSortPop(pop, exprs, orders_asc, DEFAULT_SORT_BUFFER_SIZE, DEFAULT_SORT_LAST_BUFFER_SIZE), orders_asc

    @classmethod
    def add_smjoin(cls, context: StatementContext, left: QPop,
                   alias: str, table: BaseTableLop,
                   left_exprs: list[ValExpr], right_exprs: list[ValExpr],
                   cond_remainder: ValExpr | None) -> QPop:
        pop: QPop[QPop.CompiledProps] = TableScanPop(context, alias, table.base_metadata, table.return_row_id)
        orders_asc_required: list[bool | None] = [None] * len(left_exprs)
        orders_asc: list[bool] | None
        # some optimizations to pick preferred sorting order:
        if (orders_asc := left.compiled.is_ordered(left_exprs, orders_asc_required)) is not None:
            orders_asc_required = list(orders_asc)
        elif (orders_asc := pop.compiled.is_ordered(right_exprs, orders_asc_required)) is not None:
            orders_asc_required = list(orders_asc)
        elif table.base_metadata.primary_key_column_index is not None and \
            (pki := valexpr.find_column_in_exprs(alias, table.base_metadata.id_name(), right_exprs)) is not None:
            # table is already sorted by primary key, and it's involved in join, so make this the most sigificant order
            left_exprs.insert(0, left_exprs.pop(pki))
            right_exprs.insert(0, right_exprs.pop(pki))
            orders_asc_required[0] = True
        # now let's do it:
        left, orders_asc = cls.ensure_sorted(context, left, left_exprs, orders_asc_required)
        pop, _ = cls.ensure_sorted(context, pop, right_exprs, cast(list[bool | None], orders_asc))
        pop = MergeEqJoinPop(left, pop, left_exprs, right_exprs, orders_asc)
        if cond_remainder is not None:
            pop = FilterPop(pop, cond_remainder)
        return pop
    
    @classmethod
    def add_hashjoin(cls, context: StatementContext, left: QPop, 
                     alias: str, table: BaseTableLop,
                     left_exprs: list[ValExpr], right_exprs: list[ValExpr], 
                     cond_remainder: ValExpr | None) -> QPop:
        pop: QPop[QPop.CompiledProps] = TableScanPop(context, alias, table.base_metadata, table.return_row_id)
        pop = HashEqJoinPop(left, pop, left_exprs, right_exprs, DEFAULT_HASH_BUFFER_SIZE)
        if cond_remainder is not None:
            pop = FilterPop(pop, cond_remainder)
        return pop

    @classmethod
    def _gen_sarg(cls, inner_table_alias: str, column_name: str, candidates: list[valexpr.binary.CompareOpValExpr]) \
            -> tuple[QPop.Sarg, list[valexpr.binary.CompareOpValExpr]]:
        """A helper for :meth:`.sarg_cond`.
        Given an B+tree index on ``inner_table_alias.column_name`` and ``candidates``,
        a list of sargable conditions (all of which are supposed to hold),
        generate the "best" sarg, and return the subset of candidates covered by this sarg.
        Here we assume each candidate is already pre-checked to be sargable.
        """
        sarg: Final = QPop.Sarg(
            is_range = False,
            key_lower = None, key_upper = None,
            lower_exclusive = False, upper_exclusive = False)
        covered_candidates: list[valexpr.binary.CompareOpValExpr] = list()
        for cond in candidates:
            left = cond.left()
            if isinstance(left, valexpr.leaf.NamedColumnRef) \
                and left.table_alias == inner_table_alias \
                and left.column_name == column_name:
                bound = cond.right()
            else:
                bound = cond.left()
            if isinstance(cond, valexpr.binary.EQ):
                # EQ is always better; just ignore whatever we had before
                sarg.is_range = False
                sarg.key_lower, sarg.key_upper = bound, bound
                sarg.lower_exclusive, sarg.upper_exclusive = False, False
                covered_candidates = [cond]
            elif ((isinstance(cond, valexpr.binary.GE) or isinstance(cond, valexpr.binary.GT)) and bound == cond.right()) \
              or (bound == cond.left() and (isinstance(cond, valexpr.binary.LE) or isinstance(cond, valexpr.binary.LT))):
                # we have a lower bound
                if sarg.key_lower is not None:
                    # there was a bound already; just use this old one and skip this
                    continue
                sarg.is_range = True
                sarg.key_lower = bound
                sarg.lower_exclusive = isinstance(cond, valexpr.binary.GT) or isinstance(cond, valexpr.binary.LT)
                covered_candidates.append(cond)
            elif ((isinstance(cond, valexpr.binary.LE) or isinstance(cond, valexpr.binary.LT)) and bound == cond.right()) \
              or (bound == cond.left() and (isinstance(cond, valexpr.binary.GE) or isinstance(cond, valexpr.binary.GT))):
                # we have an upper bound
                if sarg.key_upper is not None:
                    # there was a bound already; just use this old one and skip this
                    continue
                sarg.is_range = True
                sarg.key_upper = bound
                sarg.upper_exclusive = isinstance(cond, valexpr.binary.GT) or isinstance(cond, valexpr.binary.LT)
                covered_candidates.append(cond)
        return sarg, covered_candidates

    @classmethod
    def sarg_cond(cls, outer_table_aliases: list[str],
                  inner_table_alias: str, inner_table: BaseTableLop,
                  cond: ValExpr) \
        -> tuple[int, QPop.Sarg, ValExpr | None] | None:
        """Consider a base table with alias ``inner_table_alias`` and metadata ``inner_table_metadata``,
        which is joined with a collection of outer tables with ``outer_table_aliases``,
        or is by itself (if ``outer_table_aliases`` is empty).
        Given condition ``cond``, find a good :class:`.Sarg` that can be applied
        to some an index (either primary or secondary) of the base table.
        Return a triple consisting of
        1) the chosen index (identified by the column index for the index key),
        2) the ``Sarg``, and
        3) a "remainder" condition (or ``None`` if not needed) such that
        ANDing it with the ``Sarg`` is equivalent to the given condition.
        If nothing is sargable, return ``None`` instead of a triple.

        We assume that ``cond`` can be evaluated over ``outer_table_aliases`` plus ``inner_table_alias``.
        """
        # first, let's see what indexes we have on inner:
        indexed_column_names = list()
        if inner_table.base_metadata.primary_key_column_index is not None:
            indexed_column_names.append(inner_table.base_metadata.id_name())
        for i in inner_table.base_metadata.secondary_column_indices:
            indexed_column_names.append(inner_table.base_metadata.column_names[i])
        # analyze each part and attach candidate parts to indexed columns:
        parts = list(valexpr.conjunctive_parts(cond))
        candidates_map: dict[str, list[valexpr.binary.CompareOpValExpr]] = dict()
        for part in parts:
            if not isinstance(part, valexpr.binary.CompareOpValExpr):
                continue
            if isinstance(part, valexpr.binary.NE):
                # can't map <> to a single range and the condition is likely too loose anyway
                continue
            if not any(valexpr.in_scope(c, outer_table_aliases) for c in part.children()):
                # to be sargable, at least one side of the comparison must not depend on inner
                continue
            for c in part.children():
                if not isinstance(c, valexpr.leaf.NamedColumnRef):
                    continue
                if c.table_alias != inner_table_alias or c.column_name not in indexed_column_names:
                    continue
                # okay, we have a candidate associated with and indexed column:
                if c.column_name not in candidates_map:
                    candidates_map[c.column_name] = list()
                candidates_map[c.column_name].append(part)
        # pick the best candidate: we prefer EQ and then primary key;
        # otherwise the choice is arbitrary.  this isn't necessarily the best strategy.
        best_column_name = None
        best_sarg = None
        best_covered_parts = None
        for column_name, candidates in candidates_map.items():
            sarg, covered_parts = cls._gen_sarg(inner_table_alias, column_name, candidates)
            replace = False
            if best_sarg is None:
                replace = True
            elif best_sarg.is_range and not sarg.is_range:
                replace = True
            elif best_sarg.is_range == sarg.is_range and \
                column_name == inner_table.base_metadata.id_name():
                replace = True
            if replace:
                best_column_name, best_sarg, best_covered_parts = column_name, sarg, covered_parts
        if best_column_name is None or best_sarg is None or best_covered_parts is None:
            return None
        else:
            best_column_index = inner_table.base_metadata.column_names.index(best_column_name)
            remaining_parts = [part for part in parts if part not in best_covered_parts]
            return best_column_index, best_sarg, \
                (cond if len(best_covered_parts) == 0 else valexpr.make_conjunction(remaining_parts))

    @classmethod
    def join_with_base(cls, context: StatementContext, pop: QPop,
                       alias: str, table: BaseTableLop,
                       cond: ValExpr | None) -> QPop:
        """Given a ``Pop`` producing the primary key or row id column for table with ``alias``,
        return a new plan that retrieves the rest of the columns for ``alias`` using an index nested-loop join with the base table.
        Additionally apply ``cond``.
        """
        pop_base = IndexScanPop(context, alias, table.base_metadata, table.base_metadata.id_name(), is_range=False)
        key = valexpr.leaf.NamedColumnRef(alias, table.base_metadata.id_name(), table.base_metadata.id_type())
        return IndexNLJoinPop(
            pop, pop_base,
            QPop.Sarg(is_range = False, key_lower = key, key_upper = key, lower_exclusive = False, upper_exclusive = False),
            cond)

    @classmethod
    def make_independent_index_scan(cls, context: StatementContext,
                                    alias: str, table: BaseTableLop,
                                    column_index: int, sarg: QPop.Sarg, cond_remainder: ValExpr | None) -> QPop:
        pop: QPop = IndexScanPop(
            context, alias, table.base_metadata, table.base_metadata.column_names[column_index],
            is_range = cast(bool, sarg.is_range))
        # table all by itself; the sarg should have no column references, so we can evaluate and set at compile-time:
        key_lower = valexpr.eval_literal(sarg.key_lower) if sarg.key_lower is not None else None
        key_upper = valexpr.eval_literal(sarg.key_upper) if sarg.key_upper is not None else None
        cast(IndexScanPop, pop).set_range(key_lower, key_upper, sarg.lower_exclusive, sarg.upper_exclusive)
        if column_index != table.base_metadata.primary_key_column_index:
            # secondary index only, need to get the rest of the row:
            pop = cls.join_with_base(context, pop, alias, table, cond_remainder)
            cond_remainder = None
        if cond_remainder is not None:
            # apply any remaining condition:
            pop = FilterPop(pop, cond_remainder)
        return pop

    @classmethod
    def add_dependent_index_scan(cls, context: StatementContext, left: QPop,
                                 alias: str, table: BaseTableLop,
                                 column_index: int, sarg: QPop.Sarg, cond_remainder: ValExpr | None) -> QPop:
        pop: QPop = IndexScanPop(
            context, alias, table.base_metadata, table.base_metadata.column_names[column_index],
            is_range = cast(bool, sarg.is_range))
        if column_index != table.base_metadata.primary_key_column_index:
            # inner (right) is a secondary index only:
            pop = IndexNLJoinPop(left, cast(IndexScanPop, pop), sarg, None)
            # still need to join with the base table to get the full row:
            pop = cls.join_with_base(context, pop, alias, table, cond_remainder)
        else: # inner (right) is a primary index:
            pop = IndexNLJoinPop(left, cast(IndexScanPop, pop), sarg, cond_remainder)
        return pop

    @classmethod
    def optimize_one_more_table(cls, context: StatementContext, left: QPop | None, left_aliases: list[str],
                                alias: str, table: BaseTableLop, cond: ValExpr | None) \
    -> QPop:
        """Given an existing plan (``left``, containing table aliases ``left_aliases``),
        one more table (with ``alias``) to be joined,
        and a ``cond`` that can be evaluated over all of them,
        return a plan that further incorporates then given table and evaluates the given condition.
        """
        # use an index scan if possible:
        sarg_cond_out = None if cond is None \
            else cls.sarg_cond(left_aliases, alias, table, cond)
        if sarg_cond_out is not None:
            column_index, sarg, cond_remainder = sarg_cond_out
            if left is None:
                return cls.make_independent_index_scan(context, alias, table, column_index, sarg, cond_remainder)
            elif Planner.options.index_join:
                return cls.add_dependent_index_scan(context, left, alias, table, column_index, sarg, cond_remainder)
        # use sort merge join if possible:
        if Planner.options.sort_merge_join:
            eqj_cond_out = None if cond is None or left is None \
                else cls.eqj_cond(left_aliases, alias, table, cond)
            if eqj_cond_out is not None: # use a sort-merge join
                left_exprs, right_exprs, cond_remainder = eqj_cond_out
                pop = cls.add_smjoin(context, cast(QPop, left), alias, table, left_exprs, right_exprs, cond_remainder)
                return pop
        # use hash join if possible:
        if Planner.options.hash_join:
            eqj_cond_out = None if cond is None or left is None \
                else cls.eqj_cond(left_aliases, alias, table, cond)
            if eqj_cond_out is not None: # use a hash join
                left_exprs, right_exprs, cond_remainder = eqj_cond_out
                pop = cls.add_hashjoin(context, cast(QPop, left), alias, table, left_exprs, right_exprs, cond_remainder)
                return pop
        # fall back: use a table scan
        pop = TableScanPop(context, alias, table.base_metadata, table.return_row_id)
        if left is None:
            if cond is not None:
                pop = FilterPop(pop, cond)
        else:
            pop = BNLJoinPop(left, pop, cond, DEFAULT_BNLJ_BUFFER_SIZE)
        return pop

    @classmethod
    def optimize_block(cls, context: StatementContext, block: SFWGHLop) -> QPop:
        plan: QPop | None = None
        cond: ValExpr | None = block.where_cond
        outer_table_aliases: list[str] = list()
        for input_table, input_alias in zip(block.from_tables, block.from_aliases):
            if not isinstance(input_table, BaseTableLop):
                raise PlannerException('subqueries in FROM not supported')
            local_cond, cond = valexpr.push_down_conds(cond, outer_table_aliases + [input_alias]) if cond is not None else (None, None)
            plan = cls.optimize_one_more_table(context, plan, outer_table_aliases,
                                               input_alias, input_table, local_cond)
            outer_table_aliases.append(input_alias)
        if plan is None:
            raise PlannerException('unexpected error')
        if cond is not None:
            plan = FilterPop(plan, cond)
        if block.groupby_valexprs is not None:
            plan, groupby_indcies = add_groupby_by_sorting(plan, block.groupby_valexprs)
            plan = add_having_and_select(
                plan, block.groupby_valexprs, groupby_indcies,
                block.having_cond, block.select_valexprs, block.select_aliases)
        else:
            plan = ProjectPop(plan, block.select_valexprs, block.select_aliases)
        return plan
