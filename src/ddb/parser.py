import sqlglot
from sqlglot import exp

SQL_DIALECT = sqlglot.Dialects.POSTGRES

class ParserException(Exception):
    pass

def parse(sql_str: str) -> exp.Expression:
    try:
        return sqlglot.parse_one(sql_str, dialect=SQL_DIALECT)
    except sqlglot.ParseError as e:
        raise ParserException('syntax error') from e

def parse_all(sql_str: str) -> list[exp.Expression]:
    try:
        trees = sqlglot.parse(sql_str, dialect=SQL_DIALECT)
        if trees is not None:
            return list(tree for tree in trees if tree is not None)
        else:
            return list()
    except sqlglot.ParseError as e:
        raise ParserException('syntax error') from e
