from typing import cast, Sequence

from ..globals import DEFAULT_SORT_BUFFER_SIZE
from ..validator import ValExpr, valexpr, OutputLineage
from ..executor import QPop, MergeSortPop, AggrPop, FilterPop, ProjectPop

def add_groupby_by_sorting(input: QPop[QPop.CompiledProps], groupby_exprs: list[ValExpr]) -> tuple[QPop[QPop.CompiledProps], list[int]]:
    """Add additional operators on top of ``input`` as needed so that
    its output will contain all GROUP BY expressions and
    its output rows sorted by them such that those in the same group will be consecutive.
    Return the new plan, together with the list of output column indices corresponding to the GROUP BY expressions.
    However, no particular column or sort ordering is guaranteed.
    """
    # figure out what GROUP BY expressions (if any) are not merely column references,
    # and add them as extra columns using a projection as needed:
    appended_columns: list[ValExpr] = list()
    column_index_offset = len(input.compiled.output_metadata.column_names)
    groupby_column_indices: list[int] = list()
    for g in groupby_exprs:
        if (column_index := input.column_in_output(g)) is None:
            appended_columns.append(g)
            groupby_column_indices.append(column_index_offset)
            column_index_offset += 1
        else:
            groupby_column_indices.append(column_index)
    if len(appended_columns) > 0:
        project_exprs: list[ValExpr] = list()
        for i, column_type in enumerate(input.compiled.output_metadata.column_types):
            project_exprs.append(valexpr.RelativeColumnRef(0, i, column_type))
        for i, appended_expr in enumerate(appended_columns):
            project_exprs.append(appended_expr)
        input = ProjectPop(input, project_exprs, None)
    # as an optimization, see if some columns are already ordered,
    # so we can avoid a sort altogether:
    if len(appended_columns) == 0 and\
        all(column_index in input.compiled.ordered_columns\
            for column_index in groupby_column_indices):
        return input, groupby_column_indices
    # now, add the sort:
    sort_exprs: list[ValExpr] = list()
    orders_asc: list[bool] = list()
    for column_index in groupby_column_indices:
        sort_exprs.append(valexpr.RelativeColumnRef(
            0, column_index, input.compiled.output_metadata.column_types[column_index]))
        orders_asc.append(True)
    input = MergeSortPop(input, sort_exprs, orders_asc, DEFAULT_SORT_BUFFER_SIZE, DEFAULT_SORT_BUFFER_SIZE)
    return input, groupby_column_indices

def add_having_and_select(
        input: QPop[QPop.CompiledProps],
        groupby_exprs: list[ValExpr], groupby_column_indices: list[int],
        having_cond: ValExpr|None,
        select_exprs: list[ValExpr], select_aliases: Sequence[str|None] | None) -> QPop[QPop.CompiledProps]:
    # first, collect all aggregate subexpressions and make an AggrPop to compute them:
    aggr_exprs: list[valexpr.AggrValExpr] = list()
    for expr in ([] if having_cond is None else [having_cond]) + select_exprs:
        for e in valexpr.find_aggrs(expr):
            if not any(valexpr.must_be_equivalent(e, a) for a in aggr_exprs):
                aggr_exprs.append(e)
    relativized_groupby_exprs = [
        cast(ValExpr, valexpr.RelativeColumnRef(0, column_index, groupby_expr.valtype()))
        for column_index, groupby_expr in zip(groupby_column_indices, groupby_exprs)
    ]
    input = AggrPop(input, relativized_groupby_exprs, aggr_exprs, None,
                    3 * sum(not aggr.is_incremental() for aggr in aggr_exprs))
    computed_exprs = groupby_exprs + aggr_exprs
    no_lineage: OutputLineage = [set()] * len(computed_exprs)
    # next, apply HAVING:
    if having_cond is not None:
        relativized_having_cond = cast(ValExpr, valexpr.relativize(having_cond, [no_lineage], [computed_exprs]))
        input = FilterPop(input, relativized_having_cond)
    # finally, apply SELECT:
    relativized_select_exprs = [
        cast(ValExpr, valexpr.relativize(e, [no_lineage], [computed_exprs]))
        for e in select_exprs
    ]
    return ProjectPop(input, relativized_select_exprs, select_aliases)
