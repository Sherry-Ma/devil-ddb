from typing import cast, Final
import logging

from sqlglot import exp
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify import qualify

from ..types import ValType, RowType
from ..metadata import MetadataManager, BaseTableMetadata
from ..transaction import Transaction
from ..parser import parse

from .interface import ValidatorException, Lop, QLop
from .lops import BaseTableLop, SFWGHLop, InsertLop, DeleteLop, CreateTableLop, ShowTablesLop, AnalyzeStatsLop, CreateIndexLop, LiteralTableLop, SetOptionLop, CommitLop, RollbackLop
from .valexpr import ValExpr, LiteralNumber, LiteralString, LiteralBoolean, NamedColumnRef, binary, unary, func, eval_literal

def validate(mm: MetadataManager, tx: Transaction, parse_tree: exp.Expression) -> Lop:
    # normalize table/column names first:
    parse_tree = normalize_identifiers(parse_tree)
    if isinstance(parse_tree, exp.Create) and parse_tree.args['kind'] == 'TABLE':
        return validate_create_table(mm, tx, parse_tree)
    elif isinstance(parse_tree, exp.Command) and parse_tree.this == 'SHOW' and parse_tree.expression.name.lower() == 'tables':
        return ShowTablesLop()
    elif isinstance(parse_tree, exp.Command) and parse_tree.this == 'ANALYZE':
        return validate_analyze(mm, tx, parse_tree)
    elif isinstance(parse_tree, exp.Create) and parse_tree.args['kind'] == 'INDEX':
        return validate_create_index(mm, tx, parse_tree)
    elif isinstance(parse_tree, exp.Insert):
        return validate_insert(mm, tx, parse_tree)
    elif isinstance(parse_tree, exp.Delete):
        return validate_delete(mm, tx, parse_tree)
    elif isinstance(parse_tree, exp.Select):
        return validate_select(mm, tx, parse_tree)
    elif isinstance(parse_tree, exp.Command) and parse_tree.this.upper() == 'SET':
        return validate_set_option(mm, tx, parse_tree)
    elif isinstance(parse_tree, exp.Commit):
        return CommitLop()
    elif isinstance(parse_tree, exp.Rollback):
        return RollbackLop()
    else:
        raise ValidatorException('construct currently not supported')

def validate_type(type: exp.DataType.Type) -> ValType:
    if type == exp.DataType.Type.INT:
        return ValType.INTEGER
    elif type == exp.DataType.Type.FLOAT:
        return ValType.FLOAT
    elif type == exp.DataType.Type.VARCHAR:
        return ValType.VARCHAR
    elif type == exp.DataType.Type.DATETIME:
        return ValType.DATETIME
    elif type == exp.DataType.Type.BOOLEAN:
        return ValType.BOOLEAN
    else:
        raise ValidatorException(f'type {type} currently not supported')

def validate_create_table(mm: MetadataManager, tx: Transaction, parse_tree: exp.Create) -> CreateTableLop:
    table_name = cast(exp.Table, parse_tree.find(exp.Table)).this.name
    if table_name.startswith('.'):
        raise ValidatorException(f'table name cannot start with a dot')
    if mm.get_base_table_metadata(tx, table_name) is not None:
        raise ValidatorException(f'{table_name} already exists')
    column_names = list()
    column_types = list()
    for column_def in parse_tree.find_all(exp.ColumnDef):
        if column_def.this.name.startswith('.'):
            raise ValidatorException(f'column name cannot start with a dot')
        column_names.append(column_def.this.name)
        if cast(exp.DataType, column_def.args['kind']).find(exp.DataTypeParam):
            raise ValidatorException('parameterization for data types currently not supported')
        type = column_def.args['kind'].this
        column_types.append(validate_type(type))
    if len(column_names) != len(set(column_names)):
        raise ValidatorException('duplicate column names in CREATE TABLE')
    if parse_tree.find(exp.ColumnConstraint) is not None:
        raise ValidatorException('column constraint in CREATE TABLE currently not supported')
    primary_key = parse_tree.find(exp.PrimaryKey)
    if primary_key is not None:
        if len(primary_key.expressions) > 1:
            raise ValidatorException('multi-column primary key currently not supported')
        primary_key_column = primary_key.expressions[0].name
        if primary_key_column not in column_names:
            raise ValidatorException(f'primary key column {primary_key_column} not declared in CREATE TABLE')
        primary_key_column_index = column_names.index(primary_key_column)
        if column_types[primary_key_column_index] not in (ValType.INTEGER, ValType.VARCHAR):
            raise ValidatorException(f'index key type {column_types[primary_key_column_index].name} currently not supported')
    else:
        primary_key_column_index = None
    return CreateTableLop(BaseTableMetadata(column_names = column_names,
                                            column_types = column_types,
                                            name = table_name,
                                            primary_key_column_index = primary_key_column_index,
                                            secondary_column_indices = list()))

def validate_analyze(mm: MetadataManager, tx: Transaction, parse_tree: exp.Command) -> AnalyzeStatsLop:
    if (t := parse_tree.find(exp.Literal)) is not None:
        base_metas: list[BaseTableMetadata] = list()
        for table_name in t.this.split(','):
            table_name = table_name.lower()
            if (meta := mm.get_base_table_metadata(tx, table_name)) is None:
                raise ValidatorException(f'table {table_name} does not exist')
            base_metas.append(meta)
        return AnalyzeStatsLop(base_metas)
    else:
        return AnalyzeStatsLop()

def validate_create_index(mm: MetadataManager, tx: Transaction, parse_tree: exp.Create) -> CreateIndexLop:
    table_name = cast(exp.Table, parse_tree.find(exp.Table)).this.name
    table_metadata = mm.get_base_table_metadata(tx, table_name)
    if table_metadata is None:
        raise ValidatorException(f'table {table_name} does not exist')
    columns = list(parse_tree.find_all(exp.Column))
    if len(columns) > 1:
        raise ValidatorException('multi-column index currently not supported')
    column_name = columns[0].this.name
    if column_name not in table_metadata.column_names:
        raise ValidatorException(f'column {column_name} not table {table_name}')
    column_index = table_metadata.column_names.index(column_name)
    if column_index == table_metadata.primary_key_column_index:
        raise ValidatorException(f'column {column_name} is already the primary key of {table_name}')
    if column_index in table_metadata.secondary_column_indices:
        raise ValidatorException(f'secondary index {table_name}({column_name}) already exists')
    if table_metadata.column_types[column_index] not in (ValType.INTEGER, ValType.VARCHAR):
        raise ValidatorException(f'index key type {table_metadata.column_types[column_index].name} currently not supported')
    return CreateIndexLop(table_metadata, column_index)

def gather_schema(mm: MetadataManager, tx: Transaction, parse_tree: exp.Expression) -> dict[str, BaseTableMetadata]:
    """Give a statement represented by ``parse_tree``,
    collect a metadata dictionary for all base tables referenced in the statement.
    """
    metadata: dict[str, BaseTableMetadata] = dict()
    for node in parse_tree.find_all(exp.Table):
        table_name = node.this.this 
        table_metadata = mm.get_base_table_metadata(tx, table_name)
        if table_metadata is None:
                raise ValidatorException(f'table {table_name} not found')
        else:
            metadata[table_name] = table_metadata
    return metadata

def normalize_select_by_schema(parse_tree: exp.Select, metadata: dict[str, BaseTableMetadata]) -> exp.Select:
    """Given a SELECT statement represented by ``parse_tree``
    and relevant schema information (a ``metadata`` dictionary keyed by base table names),
    use ``sqlglot`` to expand all column references (to refer to table explicity),
    expand ``*``, assign names to all output columns, do some preliminary validation,
    and return a new parse tree as the result.
    """
    try:
        t = qualify(parse_tree, schema={ k: v.columns_as_ordered_dict() for k, v in metadata.items() },
                    expand_alias_refs=True, qualify_columns=True, validate_qualify_columns=True,
                    quote_identifiers=True, identify=True)
        return cast(exp.Select, t)
    except OptimizeError as e:
        raise ValidatorException('validation error') from e

def validate_select(mm: MetadataManager, tx: Transaction, parse_tree: exp.Select) -> SFWGHLop:
    # collect metadata for all tables used, and normalize the parse tree accordingly:
    metadata = gather_schema(mm, tx, parse_tree)
    parse_tree = normalize_select_by_schema(parse_tree, metadata)
    if parse_tree.find(exp.Subquery):
        raise ValidatorException('subqueries currently not supported')
    # validate FROM:
    from_tables: list[QLop] = list()
    from_aliases = list()
    # the first item of FROM:
    if parse_tree.find(exp.From) is None:
        raise ValidatorException('FROM-less SELECT not supported')
    table_name = cast(exp.From, parse_tree.find(exp.From)).this.this.name
    from_tables.append(validate_base_table(mm, tx, table_name))
    table_alias = cast(exp.From, parse_tree.find(exp.From)).find(exp.TableAlias)
    from_aliases.append(table_name if table_alias is None else table_alias.this.name)
    # the remainder of FROM:
    for join in parse_tree.find_all(exp.Join):
        if any(i in join.args for i in ('side', 'kind', 'method', 'on', 'using')):
            raise ValidatorException('fancy joins (natural, outer, on/using, etc) currently not supported')
        table_name = join.this.this.name
        from_tables.append(validate_base_table(mm, tx, table_name))
        table_alias = join.find(exp.TableAlias)
        from_aliases.append(table_name if table_alias is None else table_alias.this.name)
    if len(from_aliases) != len(set(from_aliases)):
        raise ValidatorException('duplicate table alias in FROM')
    # validate WHERE:
    if 'where' in parse_tree.args:
        where_cond = validate_valexpr(parse_tree.args['where'].this, from_tables, from_aliases)
        if where_cond.valtype() != ValType.BOOLEAN:
            raise ValidatorException('WHERE expression is not BOOLEAN')
    else:
        where_cond = None
    # validate SELECT:
    select_valexprs = list()
    select_aliases = list()
    for node in parse_tree.expressions:
        if type(node) != exp.Alias:
            raise ValidatorException('unexpected error')
        select_valexprs.append(validate_valexpr(node.this, from_tables, from_aliases))
        select_aliases.append(node.args['alias'].name)
    return SFWGHLop(select_valexprs, select_aliases, from_tables, from_aliases, where_cond)

def validate_base_table(mm: MetadataManager, tx: Transaction, table_name: str, return_row_id: bool = False) -> BaseTableLop:
    table_metadata = mm.get_base_table_metadata(tx, table_name)
    if table_metadata is None:
        raise ValidatorException(f'table {table_name} does not exist')
    elif return_row_id:
        if table_metadata.primary_key_column_index is None:
            return BaseTableLop(table_metadata, return_row_id=True)
        else:
            raise ValidatorException(f'table {table_name} has a primary key but no internal row id')
    return BaseTableLop(table_metadata)

def validate_column_ref(table_name: str, column_name: str,
                        from_tables: list[QLop], from_aliases: list[str]) -> NamedColumnRef:
    if table_name not in from_aliases:
        raise ValidatorException(f'cannot find FROM table for column reference {table_name}.{column_name}')
    table_lop = from_tables[from_aliases.index(table_name)]
    if column_name not in table_lop.metadata().column_names:
        raise ValidatorException(f'cannot find column {table_name}.{column_name}')
    column_type = table_lop.metadata().column_types[table_lop.metadata().column_names.index(column_name)]
    return NamedColumnRef(table_name, column_name, column_type)

def validate_valexpr(tree, from_tables: list[QLop], from_aliases: list[str]) -> ValExpr:
    unary_mapping: Final[dict[type[exp.Expression], type[unary.UnaryOpValExpr]]] = {
        exp.Neg: unary.NEG,
        exp.Not: unary.NOT,
    }
    binary_mapping: Final[dict[type[exp.Expression], type[binary.BinaryOpValExpr]]] = {
        exp.Add: binary.PLUS,
        exp.Sub: binary.MINUS,
        exp.Mul: binary.MULTIPLY,
        exp.Div: binary.DIVIDE,
        exp.Mod: binary.MOD,
        exp.DPipe: binary.CONCAT,
        exp.RegexpLike: binary.REGEXPLIKE,
        exp.And: binary.AND,
        exp.Or: binary.OR,
        exp.EQ: binary.EQ,
        exp.NEQ: binary.NE,
        exp.LT: binary.LT,
        exp.LTE: binary.LE,
        exp.GT: binary.GT,
        exp.GTE: binary.GE,
    }
    func_mapping: Final[dict[type[exp.Expression], type[func.FunCallValExpr]]] = {
        exp.Lower: func.LOWER,
        exp.Upper: func.UPPER,
    }
    anon_func_mapping: Final[dict[str, type[func.FunCallValExpr]]] = {
        # these are not recognized by parser as built-in; they end up in exp.Anonymous:
        'replace': func.REPLACE,
    }
    if type(tree) in unary_mapping:
        return unary_mapping[type(tree)](validate_valexpr(tree.this, from_tables, from_aliases))
    elif type(tree) in binary_mapping:
        return binary_mapping[type(tree)](validate_valexpr(tree.this, from_tables, from_aliases),
                                          validate_valexpr(tree.expression, from_tables, from_aliases))
    elif type(tree) in func_mapping:
        func_arg_exprs = [validate_valexpr(tree.this, from_tables, from_aliases)]
        for arg in tree.expressions:
            func_arg_exprs.append(validate_valexpr(arg, from_tables, from_aliases))
        return func_mapping[type(tree)](tuple(func_arg_exprs))
    elif type(tree) == exp.Anonymous and (anon_func := anon_func_mapping.get(tree.this.lower(), None)) is not None:
        func_arg_exprs = list()
        for arg in tree.expressions:
            func_arg_exprs.append(validate_valexpr(arg, from_tables, from_aliases))
        return anon_func(tuple(func_arg_exprs))
    match type(tree):
        case exp.Paren:
            return validate_valexpr(tree.this, from_tables, from_aliases)
        case exp.Literal:
            if tree.args['is_string']:
                return LiteralString(tree.this)
            else:
                return LiteralNumber.from_str(tree.this)
        case exp.Boolean:
            return LiteralBoolean(tree.this)
        case exp.Column:
            return validate_column_ref(tree.args['table'].name, tree.this.name, from_tables, from_aliases)
        case exp.Cast:
            return func.CAST(
                (validate_valexpr(tree.this, from_tables, from_aliases), ),
                dict(AS=validate_type(tree.args['to'].this)))
        case _:
            raise ValidatorException(f'{type(tree).__name__} construct currently not supported')

def validate_values(parse_tree: exp.Values, row_type: RowType | None = None) -> tuple[RowType, list[tuple]]:
    rows: list[tuple] = list()
    type_inferred: bool = False
    for i, t in enumerate(parse_tree.find_all(exp.Tuple)):
        literals = [validate_valexpr(c, [], []) for c in t.expressions]
        if row_type is None:
            row_type = [c.valtype() for c in literals]
            type_inferred = True
        if len(literals) != len(row_type):
            raise ValidatorException(f'row {i+1} of VALUES: incorrect number of columns')
        values = list()
        for j, (c_literal, c_type) in enumerate(zip(literals, row_type)):
            if not c_literal.valtype().can_cast_to(c_type):
                raise ValidatorException(
                    f'row {i+1} column {j+1} of VALUES: {c_literal.valtype().name} not compatible with {c_type.name}' +\
                    (' in the first row' if type_inferred else ''))
            if c_literal.valtype() == c_type:
                c_val = eval_literal(c_literal)
            else:
                c_val = c_type.cast_from(eval_literal(c_literal))
            values.append(c_val)
        rows.append(tuple(values))
    if row_type is None:
        raise ValidatorException('VALUES contains now rows')
    return row_type, rows

def validate_insert(mm: MetadataManager, tx: Transaction, parse_tree: exp.Insert) -> InsertLop:
    if type(parse_tree.this) == exp.Schema:
        raise ValidatorException('inserting into a specific list of columns currently not supported')
    table_name = cast(exp.Table, parse_tree.find(exp.Table)).this.name
    table_metadata = mm.get_base_table_metadata(tx, table_name)
    if table_metadata is None:
        raise ValidatorException(f'{table_name} not found')
    if type(parse_tree.expression) == exp.Values:
        _, rows = validate_values(parse_tree.expression, table_metadata.column_types)
        return InsertLop(table_metadata, LiteralTableLop(table_metadata, rows))
    elif type(parse_tree.expression) == exp.Subquery:
        lop = validate(mm, tx, parse_tree.expression.this)
        if not isinstance(lop, QLop):
            raise ValidatorException('INSERT exepcts a table-valued subquery')
        if len(lop.metadata().column_types) != len(table_metadata.column_types):
            raise ValidatorException(f'{len(lop.metadata().column_types)} columns supplied; expecting {len(table_metadata.column_types)}')
        for i, (stype, ttype) in enumerate(zip(lop.metadata().column_types, table_metadata.column_types)):
            if not stype.can_cast_to(ttype):
                raise ValidatorException(f'cannot convert {stype.name} to {ttype.name} for the {i}-th column')
            elif not stype.implicitly_casts_to(ttype):
                raise ValidatorException(f'explicit CAST from {stype.name} to {ttype.name} needed for the {i}-th column')
        return InsertLop(table_metadata, lop)
    else:
        raise ValidatorException(f'{type(parse_tree.expression).__name__} in INSERT currently not supported')

def validate_delete(mm: MetadataManager, tx: Transaction, parse_tree: exp.Delete) -> DeleteLop:
    table_name = cast(exp.Table, parse_tree.find(exp.Table)).this.name
    base_meta = mm.get_base_table_metadata(tx, table_name)
    if base_meta is None:
        raise ValidatorException(f'{table_name} not found')
    # a hack to construct a query that computes affected rows:
    sql = f'SELECT * FROM {table_name}'
    if 'where' in parse_tree.args and parse_tree.args['where'] is not None:
        sql += ' WHERE ' + parse_tree.args['where'].this.sql()
    key_query_tree = cast(exp.Select, parse(sql))
    key_query: SFWGHLop = validate_select(mm, tx, key_query_tree)
    # compute necessary edits to the logical plan:
    # first, ensure that the base table scan gets internal row id if needed:
    if base_meta.primary_key_column_index is None:
        key_query.from_tables.clear()
        key_query.from_tables.append(validate_base_table(mm, tx, table_name, return_row_id=True))
    # second, ensure that we get first get the id (primary key or internal row id), and then all secondary key values in order:
    key_query.select_valexprs.clear()
    key_query.select_aliases.clear()
    key_query.select_valexprs.append(NamedColumnRef(base_meta.name, base_meta.id_name(), base_meta.id_type()))
    key_query.select_aliases.append('id')
    for i, sk_column_index in enumerate(base_meta.secondary_column_indices):
        key_query.select_valexprs.append(
            NamedColumnRef(base_meta.name, base_meta.column_names[sk_column_index], base_meta.column_types[sk_column_index]))
        key_query.select_aliases.append(f'sk_{i}')
    return DeleteLop(base_meta, key_query)

def validate_set_option(mm: MetadataManager, tx: Transaction, parse_tree: exp.Command) -> SetOptionLop:
    fields = str(parse_tree.expression).strip().split(maxsplit=1)
    if len(fields) != 2:
        raise ValidatorException('SET command expects a option followed by space and then its new value')
    option = fields[0].lower()
    value = ' '.join(fields[1].lower().split())
    return SetOptionLop(option, value)
