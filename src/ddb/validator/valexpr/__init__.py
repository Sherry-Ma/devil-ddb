"""The ``ValExpr.valexpr`` package contains classes and utility functions for
handling (validated) expressions that evaluate to an atomic value (as opposed to a collection).
"""
from .interface import ValExpr, ArithOpValExpr, BoolOpValExpr
from .leaf import LeafValExpr, Literal, LiteralString, LiteralNumber, LiteralBoolean, NamedColumnRef, RelativeColumnRef
from .unary import UnaryOpValExpr
from .binary import BinaryOpValExpr, CompareOpValExpr
from .util import cast_if_needed, conjunctive_parts, make_conjunction, in_scope, find_column_refs,\
    reverse_comparison, is_column_comparing_to_literal, are_columns_joining,\
    push_down_conds, find_column_in_exprs, must_be_equivalent,\
    OutputLineage, find_column_in_lineage, relativize, to_code_str, eval_literal
