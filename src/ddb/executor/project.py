from typing import cast, Final, Iterable, Sequence, Generator, Any
from dataclasses import dataclass
from functools import cached_property

from ..profile import profile_generator
from ..validator import valexpr, ValExpr, OutputLineage
from ..validator.valexpr.eval import *
from ..metadata import TableMetadata, INTERNAL_ANON_COLUMN_NAME_FORMAT, INTERNAL_ANON_TABLE_NAME_FORMAT

from .interface import QPop

class ProjectPop(QPop['ProjectPop.CompiledProps']):
    """Simple, duplicate-preserving project(ion) + optional renaming physical operator.
    No extra memory is needed.
    """

    @dataclass
    class CompiledProps(QPop.CompiledProps):
        output_codes: list[str]
        """Python code for computing each of the output columns.
        """
        output_execs: list[Any]
        """Python executable for computing each of the output columns.
        """

        def pstr_more_compiled(self) -> Iterable[str]:
            yield from super().pstr()
            for column_name, code in zip(self.output_metadata.column_names, self.output_codes):
                yield f'code for {column_name}: {code}'
            return

    def __init__(self, input: QPop[QPop.CompiledProps], exprs: list[ValExpr], column_names: Sequence[str | None] | None) -> None:
        """Construct a projection on top of the given ``input``.
        If ``columns_names`` is None or if some of its entries are None, some internal names will be assigned automatically.
        """
        super().__init__(input.context)
        self.input: Final = input
        self.output_table_name: Final[str] = INTERNAL_ANON_TABLE_NAME_FORMAT.format(pop=type(self).__name__, hex=hex(id(self)))
        self.output_exprs: Final = exprs
        self.output_column_names: Final[list[str]] = list()
        for i, (expr, column_name) in enumerate(zip(self.output_exprs, [None] * len(exprs) if column_names is None else column_names)):
            if column_name is not None:
                self.output_column_names.append(column_name)
            elif isinstance(expr, valexpr.leaf.NamedColumnRef):
                self.output_column_names.append(expr.column_name)
            else:
                self.output_column_names.append(INTERNAL_ANON_COLUMN_NAME_FORMAT.format(index = i)) # default
        return

    def memory_blocks_required(self) -> int:
        return 0

    def children(self) -> tuple[QPop[QPop.CompiledProps], ...]:
        return (self.input, )

    def pstr_more(self) -> Iterable[str]:
        yield f'AS {self.output_table_name}:'
        for expr, name in zip(self.output_exprs, self.output_column_names):
            yield f'  {name}: {expr.to_str()}'
        return

    @cached_property
    def compiled(self) -> 'ProjectPop.CompiledProps':
        input_props = self.input.compiled
        output_column_types = [e.valtype() for e in self.output_exprs]
        output_lineage: OutputLineage = list()
        preserved_input_columns: dict[int, int] = dict()
        i: int | None
        for i, (expr, output_column_name) in enumerate(zip(self.output_exprs, self.output_column_names)):
            output_column_lineage = set(((self.output_table_name, output_column_name), ))
            if (input_column_index := self.column_in_child(expr, 0)) is not None:
                output_column_lineage = output_column_lineage | input_props.output_lineage[input_column_index]
                preserved_input_columns[input_column_index] = i
            output_lineage.append(output_column_lineage)
        ordered_columns: list[int] = list()
        ordered_asc: list[bool] = list()
        for input_column_index, asc in zip(input_props.ordered_columns, input_props.ordered_asc):
            if (i := preserved_input_columns.get(input_column_index)) is not None:
                ordered_columns.append(i)
                ordered_asc.append(asc)
            else: # any "gap" means remaining columns won't be ordered
                break
        unique_columns: set[int] = set()
        for input_column_index in input_props.unique_columns:
            if (i := preserved_input_columns.get(input_column_index)) is not None:
                unique_columns = unique_columns | {i}
        output_codes = list()
        output_execs = list()
        for expr in self.output_exprs:
            code, exec = self.compile_valexpr(expr)
            output_codes.append(code)
            output_execs.append(exec)
        return ProjectPop.CompiledProps(output_metadata = TableMetadata(self.output_column_names, output_column_types),
                                        output_lineage = output_lineage,
                                        ordered_columns = ordered_columns,
                                        ordered_asc = ordered_asc,
                                        unique_columns = unique_columns,
                                        output_codes = output_codes,
                                        output_execs = output_execs)

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        stats = self.context.zm.projection_stats(
            self.input.estimated.stats,
            [ cast(ValExpr, valexpr.relativize(e, [self.input.compiled.output_lineage])) for e in self.output_exprs])
        return QPop.EstimatedProps(
            stats = stats,
            blocks = QPop.EstimatedProps.StatsInBlocks(
                self_reads = 0,
                self_writes = 0,
                overall = self.input.estimated.blocks.overall))

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        output_execs = self.compiled.output_execs
        for row in self.input.execute():
            yield tuple(eval(exec, None, dict(row0 = row)) for exec in output_execs)
        return
