from typing import cast, Final, Iterable, Generator, Any
from dataclasses import dataclass
from functools import cached_property
import logging
import ctypes
import hashlib
from sys import getsizeof
from math import log, floor

from ...profile import profile_generator
from ...validator import ValExpr, valexpr
from ...validator.valexpr.eval import *
from ...storage import HeapFile
from ...globals import BLOCK_SIZE, DEFAULT_HASH_MAX_DEPTH

from ..util import BufferedWriter
from ..interface import QPop

from .interface import JoinPop

class HashEqJoinPop(JoinPop['HashEqJoinPop.CompiledProps']):
    """Hash join physical operator.
    The left input will be used as the build table and the right as the probe table.
    It will use as many memory blocks as it is given.
    """

    @dataclass
    class CompiledProps(QPop.CompiledProps):
        eq_code: str
        """Python code for the join condition.
        """
        eq_exec: Any
        """Python executable for the join condition.
        """
        left_join_vals_code: str
        """Python code for extracting the join expression values from the left input.
        If there are multiple join expression, this will return a tuple rather than a single value.
        """
        left_join_vals_exec: Any
        """Python executable for extracting the join expression values from the left input.
        """
        right_join_vals_code: str
        """Python code for extracting the join expression values from the right input.
        If there are multiple join expression, this will return a tuple rather than a single value.
        """
        right_join_vals_exec: Any
        """Python executable for extracting the join expression values from the right input.
        """

        def pstr(self) -> Iterable[str]:
            yield from super().pstr()
            yield 'join condition code: ' + self.eq_code
            return

    def __init__(self, left: QPop[QPop.CompiledProps], right: QPop[QPop.CompiledProps],
                 left_exprs: list[ValExpr],
                 right_exprs: list[ValExpr],
                 num_memory_blocks: int) -> None:
        """Construct a hash join between ``left`` and ``right`` inputs on the specified expressions
        (most commonly columns): ``left_exprs`` and ``right_exprs`` are to be evaluated over each row
        from left input and each row from right input, respectively.
        """
        super().__init__(left, right)
        self.left_exprs: Final = left_exprs
        self.right_exprs: Final = right_exprs
        self.num_memory_blocks: Final = num_memory_blocks
        return

    def memory_blocks_required(self) -> int:
        return self.num_memory_blocks

    def pstr_more(self) -> Iterable[str]:
        for left_expr, right_expr in zip(self.left_exprs, self.right_exprs):
            yield f'{left_expr.to_str()} = {right_expr.to_str()}'
        yield f'# memory blocks: {self.num_memory_blocks}'
        return

    @cached_property
    def compiled(self) -> 'HashEqJoinPop.CompiledProps':
        # ordered columns: 
        # if there is at least one pair of joined columns are both unique, all the uniqueness stay the same;
        # otherwise, nothing is guaranteed to remain unique.
        left_props = self.left.compiled
        right_props = self.right.compiled
        unique_columns: set[int] = set()
        col_i_offset = len(left_props.output_metadata.column_names)
        both_unique_flag = False
        for left_expr, right_expr in zip(self.left_exprs, self.right_exprs):
            if self.column_in_child(left_expr, 0) in left_props.unique_columns and \
            self.column_in_child(right_expr, 1) in right_props.unique_columns:
                # we got one:
                both_unique_flag = True
                break
        if both_unique_flag:
            unique_columns |= left_props.unique_columns
            unique_columns |= set(col_i_offset + col_i for col_i in right_props.unique_columns)
        # eq_code is a sequence of equality comparisons connected by Python 'and':
        eq_codes: list[str] = list()
        left_signature_codes: list[str] = list()
        right_signature_codes: list[str] = list()
        for left_expr, right_expr in zip(self.left_exprs, self.right_exprs):
            this_code, _ = self.compile_valexpr(left_expr, ['this', 'that'])
            that_code, _ = self.compile_valexpr(right_expr, ['this', 'that'])
            eq_codes.append(f'{this_code}=={that_code}')
            left_signature_codes.append(this_code)
            right_signature_codes.append(that_code)
        eq_code = ' and '.join(eq_codes)
        eq_exec = compile(eq_code, '<string>', 'eval')
        # left/right joined tuple represents the joined columns/expressions that are wrapped up by a python 'tuple'
        left_join_vals_code = '(' + ','.join(left_signature_codes) + ')'
        left_join_vals_exec = compile(left_join_vals_code, '<string>', 'eval')
        right_join_vals_code = '(' + ','.join(right_signature_codes) + ')'
        right_join_vals_exec = compile(right_join_vals_code, '<string>', 'eval')
        return HashEqJoinPop.CompiledProps.from_inputs(
            self.left.compiled, self.right.compiled,
            unique_columns = unique_columns,
            eq_code = eq_code,
            eq_exec = eq_exec,
            left_join_vals_code = left_join_vals_code,
            left_join_vals_exec = left_join_vals_exec,
            right_join_vals_code = right_join_vals_code,
            right_join_vals_exec = right_join_vals_exec)

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        relativized_equalities = [
            cast(ValExpr, valexpr.relativize(
                valexpr.binary.EQ(e1, e2),
                [self.left.compiled.output_lineage, self.right.compiled.output_lineage]))
            for e1, e2 in zip(self.left_exprs, self.right_exprs)
        ]
        stats = self.context.zm.join_stats(
            self.left.estimated.stats,
            self.right.estimated.stats,
            valexpr.make_conjunction(relativized_equalities))
        # make some guess about how many reads/writes are needed:
        left_blocks = self.left.estimated.stats.block_count()
        right_blocks = self.right.estimated.stats.block_count()
        # number of partitoning passes can be as few as zero:
        estimated_passes = floor(log(left_blocks, self.num_memory_blocks - 1))
        reads = (left_blocks + right_blocks) * estimated_passes
        writes = (left_blocks + right_blocks) * estimated_passes
        return QPop.EstimatedProps(
            stats = stats,
            blocks = QPop.StatsInBlocks(
                self_reads = reads,
                self_writes = writes,
                overall = self.left.estimated.blocks.overall + self.right.estimated.blocks.overall +\
                    reads + writes))

    def _tmp_partition_file(self, side: str, depth: int, partition_id: int) -> HeapFile:
        """Create a temporary file for a partition in a given side (left/right), an ordinal depth, a partition id
        """
        f = self.context.sm.heap_file(self.context.tmp_tx, f'.tmp-{hex(id(self))}-{side}-{depth}-{partition_id}', [], create_if_not_exists=True)
        f.truncate()
        return f

    @staticmethod
    def hash(v: Any) -> int:
        # Python's hash() may give a negative integer, and it's just identity for integers, so let's scramble it more:
        x = ctypes.c_uint32(hash(v)).value
        x = ((x >> 16) ^ x) * 0x45d9f3b
        x = ((x >> 16) ^ x) * 0x45d9f3b
        x = (x >> 16) ^ x
        return x
        # a more heavy-weight alternative:
        # return int.from_bytes(hashlib.sha256(str(v).encode('utf-8')).digest(), 'big')

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        depth = 0
        num_partitions = 1
        sides = ['this', 'that']
        join_vals_execs = [self.compiled.left_join_vals_exec, self.compiled.right_join_vals_exec]
        build_side = 0 # 0 indicates left (this) and 1 indicates right (that)
        partitions: list[list[HeapFile|QPop]] = [[self.left], [self.right]] # one list for left and one for right
        while depth < DEFAULT_HASH_MAX_DEPTH:
            logging.debug(f'***** partitioning pass {depth}')
            fanout = self.num_memory_blocks if depth == 0 else self.num_memory_blocks - 1
            old_num_partitions = num_partitions
            num_partitions = old_num_partitions * fanout # conceptually, to used to mod the hash value
            old_partitions = partitions
            partitions = [list(), list()]
            # note that the list won't be sorted by the mod value, but rather in a component-reversed order:
            # for example, say each pass i does a 10-way partition and uses % 10^(i+1),
            # then at the end of pass 2, ***110 would be stored in the 011-th partition in the list.
            # this ordering ensures that each partition from the previous pass
            # gets further divided into a contiguous range of partitions in the new pass.
            max_partition_sizes = [0, 0]
            for ci, join_vals_exec in enumerate(join_vals_execs):
                for old_i, old_partition in enumerate(old_partitions[ci]):
                    # since we mod the hash value, a row in old partition old_i must be in
                    # one of new partitions between old_i*fanout and (old_i+1)*fanout-1;
                    # let's create these new partitions and writers:
                    writers: list[BufferedWriter] = list()
                    partition_sizes = list()
                    for j in range(fanout):
                        partition = self._tmp_partition_file(sides[ci], depth, old_i*fanout+j)
                        partitions[ci].append(partition)
                        writers.append(BufferedWriter(partition, 1))
                        partition_sizes.append(0)
                    for row in (old_partition.execute() if isinstance(old_partition, QPop) else
                                old_partition.iter_scan()): # iter_scan() needs 1 memory block
                        join_vals = eval(join_vals_exec, None, {sides[ci]: row})
                        h = HashEqJoinPop.hash(join_vals) // old_num_partitions # ignore previously used bits
                        partition_sizes[h % fanout] += getsizeof(row)
                        writers[h % fanout].write(row)
                    for writer in writers:
                        writer.flush()
                    # remove old partition:
                    if isinstance(old_partition, HeapFile):
                        self.context.sm.delete_heap_file(self.context.tmp_tx, old_partition.name)
                    # update max size:
                    max_partition_sizes[ci] = max(max_partition_sizes[ci], max(partition_sizes))
            # check if max partition size is small enough for join/probe:
            if max_partition_sizes[0] <= (self.num_memory_blocks - 1) * BLOCK_SIZE:
                build_side = 0
                break
            elif max_partition_sizes[1] <= (self.num_memory_blocks - 1) * BLOCK_SIZE:
                build_side = 1
                break
            depth += 1
        logging.debug('***** probing/joining pass')
        for build, probe in zip(partitions[build_side], partitions[1-build_side]):
            # build:
            build_rows_by_join_vals: dict[Any, list[tuple]] = dict()
            join_vals_exec = join_vals_execs[build_side]
            for row in (build.execute() if isinstance(build, QPop) else
                        build.iter_scan()): # iter_scan() needs 1 memory block
                join_vals = eval(join_vals_exec, None, {sides[build_side]: row})
                if join_vals not in build_rows_by_join_vals:
                    build_rows_by_join_vals[join_vals] = list()
                build_rows_by_join_vals[join_vals].append(row)
            # remove build partition:
            if isinstance(build, HeapFile):
                self.context.sm.delete_heap_file(self.context.tmp_tx, build.name)
            if len(build_rows_by_join_vals) > 0:
                # stream in probe:
                join_vals_exec = join_vals_execs[1-build_side]
                for row in (probe.execute() if isinstance(probe, QPop) else
                            probe.iter_scan()): # iter_scan() needs 1 memory block
                    join_vals = eval(join_vals_exec, None, {sides[1-build_side]: row})
                    if join_vals not in build_rows_by_join_vals:
                        continue # nothing can be possibly joined
                    for build_row in build_rows_by_join_vals[join_vals]:
                        if eval(self.compiled.eq_exec, None, {sides[build_side]: build_row, sides[1-build_side]: row}):
                            yield (*build_row, *row) if build_side == 0 else (*row, *build_row)
            # remove probe partition:
            if isinstance(probe, HeapFile):
                self.context.sm.delete_heap_file(self.context.tmp_tx, probe.name)
        return
