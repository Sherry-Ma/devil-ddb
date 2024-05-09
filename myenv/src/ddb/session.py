from typing import cast, Final, Type, Self, Iterable, TextIO, TYPE_CHECKING
from dataclasses import dataclass, field
from sqlglot import exp
import traceback
import logging

from .globals import ANSI
from .util import OptionsBase
from .parser import parse_all, ParserException
from .validator import validate, ValidatorException, SetOptionLop, CommitLop, RollbackLop
from .planner import Planner, NaivePlanner, BaselinePlanner
from .executor import StatementContext, ExecutorException, CPop, QPop
if TYPE_CHECKING:
    # this hack and the use of quoted types for forward references below
    # are required to avoid Python circular import nightmare.
    from .db import DatabaseManager
    from .transaction.baseline import LMDBTransaction

@dataclass
class Response:
    response: str | None = None
    error: str | None = None
    error_details: str | None = None

    def pstr(self) -> Iterable[str]:
        if self.response is not None:
            yield f'{ANSI.PROMPT}{self.response}{ANSI.END}'
        if self.error is not None:
            yield f'{ANSI.ERROR}{self.error}{ANSI.END}'
        if self.error_details is not None:
            yield f'{ANSI.DEMPH}{self.error_details}{ANSI.END}'
        return

class Session:

    @dataclass
    class Options(OptionsBase):
        autocommit: bool = field(default=True, metadata={'on': True, 'off': False})
        read_only: bool = field(default=False, metadata={'read only': True, 'read write': False})
        debug: bool = field(default=False, metadata={'on': True, 'off': False})
        planner: Type[Planner] = field(default=BaselinePlanner, metadata={'baseline': BaselinePlanner, 'naive': NaivePlanner})

    def __init__(self, dbm: 'DatabaseManager') -> None:
        self.dbm: Final = dbm
        self.options = Session.Options()
        self.parent_tx: 'LMDBTransaction' | None = None
        self.parent_tmp_tx: 'LMDBTransaction' | None = None
        self.parent_has_done_work: bool = False
        return

    def __enter__(self) -> Self:
        """Required for the context manager to ready this object.
        """
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        """Required for the context manager to release any resources used by this object.
        """
        if self.parent_tmp_tx is not None:
            self.parent_tmp_tx.abort()
            self.parent_tmp_tx = None
        if self.parent_tx is not None and self.parent_has_done_work:
            logging.warning('session ending with ongoing transaction')
            self.parent_tx.abort()
            logging.warning(f'ROLLED BACK {self.parent_tx}')
            self.parent_tx = None
            self.parent_has_done_work = False
        return

    def request(self, parse_tree: exp.Expression) -> Response:
        logging.debug('='*20 + ' REQUEST ' + '='*20)
        logging.debug(repr(parse_tree))
        # if needed, start parent transaction (recall we need one for database one for tmp space) spanning multiple requests:
        if not self.options.autocommit and self.parent_tx is None:
            self.parent_tx = self.dbm.tm.begin_transaction(read_only=self.options.read_only)
            self.parent_tmp_tx = self.dbm.tm.begin_transaction(read_only=False, tmp=True)
            self.parent_has_done_work = False
        # action to take on parent transaction upon completing request:
        parent_to_do = 0 # 0 means nothing; 1 means commit; -1 means rollback
        # start the (inner) transaction (again one for database and one for tmp space)
        # to handle the current request; error in this request won't abort the parent transaction (if any):
        r = Response()
        with self.dbm.tm.begin_transaction(parent=self.parent_tx, read_only=self.options.read_only) as tx, \
            self.dbm.tm.begin_transaction(parent=self.parent_tmp_tx, read_only=False, tmp=True) as tmp_tx:
            try:
                context = StatementContext(sm=self.dbm.sm, mm=self.dbm.mm, zm=self.dbm.zm, tx=tx, tmp_tx=tmp_tx)
                # validate: parse tree -> logical plan
                lop = validate(self.dbm.mm, context.tx, parse_tree)
                logging.debug('-'*20 + ' LOGICAL PLAN ' + '-'*20)
                for s in lop.pstr():
                    logging.debug(s)
                # handle special Lops that don't go through planner:
                if isinstance(lop, SetOptionLop):
                    if self.set_option(lop):
                        parent_to_do = 1 # commit upon completing request
                    r.response = 'SET'
                elif isinstance(lop, CommitLop):
                    if self.parent_tx is None:
                        raise ExecutorException('no transaction to COMMIT')
                    else:
                        r.response = 'COMMIT'
                        parent_to_do = 1 # commit upon completing request
                elif isinstance(lop, RollbackLop):
                    if self.parent_tx is None:
                        raise ExecutorException('no transaction to ROLLBACK')
                    else:
                        r.response = 'ROLLBACK'
                        parent_to_do = -1 # rollback upon completing request
                else: # plan and execute: logical plan -> response
                    pop = self.options.planner.plan(context, lop)
                    logging.debug('-'*20 + f' PHYSICAL PLAN BY {self.options.planner.__name__} ' + '-'*20)
                    if isinstance(pop, QPop):
                        logging.debug(f'total estimated I/Os: {pop.estimated_cost}; memory required: {pop.total_memory_blocks_required()}')
                    for s in pop.pstr():
                        logging.debug(s)
                    if isinstance(pop, QPop):
                        for s in cast(QPop.CompiledProps, pop.compiled).output_metadata.pstr():
                            print(f'{ANSI.EMPH}{s}{ANSI.END}')
                        count = 0
                        for row in pop.execute():
                            print(row)
                            count += 1
                        r.response = f'SELECT {count}'
                    elif isinstance(pop, CPop):
                        r.response = pop.execute()
                    else:
                        raise ExecutorException(f'unexpected error')
                tmp_tx.commit()
                tx.commit()
                if self.parent_tx is not None:
                    self.parent_has_done_work = True
                r.response = r.response + f'\nCOMMITTED {tx}'
            except Exception as e:
                r.response = f'ROLLED BACK {tx}'
                r.error = str(e)
                if e.__cause__ is not None:
                    r.error = r.error + f'\n{str(e.__cause__).strip()}'
                r.error_details = traceback.format_exc()
                tmp_tx.abort()
                tx.abort()
        if parent_to_do == 1:
            if self.parent_tx is None or self.parent_tmp_tx is None:
                raise ExecutorException('unexpected error')
            self.parent_tmp_tx.commit()
            self.parent_tmp_tx = None
            self.parent_tx.commit()
            r.response = ((r.response + '\n') if r.response is not None else '') + f'COMMITTED {self.parent_tx}'
            self.parent_tx = None
            self.parent_has_done_work = False
        elif parent_to_do == -1:
            if self.parent_tx is None or self.parent_tmp_tx is None:
                raise ExecutorException('unexpected error')
            self.parent_tmp_tx.abort()
            self.parent_tmp_tx = None
            self.parent_tx.abort()
            r.response = ((r.response + '\n') if r.response is not None else '') + f'ROLLED BACK {self.parent_tx}'
            self.parent_tx = None
            self.parent_has_done_work = False
        return r

    def set_option(self, lop: SetOptionLop) -> bool:
        """Process SET command, and return whether to commit parent.
        """
        signal_parent_commit: bool = False
        if self.options.provides(lop.option) is not None:
            if lop.option == 'autocommit':
                # only case requiring checking is off -> on:
                if not self.options.autocommit and lop.value == 'on':
                    if self.parent_has_done_work and self.parent_tx is not None:
                        raise ExecutorException('before setting AUTOCOMMIT ON, ' +\
                                                f'commit or abort ongoing transaction {self.parent_tx}')
                    else:
                        signal_parent_commit = True # commit upon completing request
            elif lop.option == 'transaction':
                # only case requiring checking is read/only -> read/write:
                if self.options.read_only and lop.value == 'read write':
                    if self.parent_has_done_work and self.parent_tx is not None:
                        raise ExecutorException('before setting TRANSACTION READ WRITE, ' +\
                                                f'commit or abort ongoing READ ONLY transaction {self.parent_tx}')
            elif lop.option == 'debug':
                if lop.value == 'on':
                    logging.getLogger().setLevel(logging.DEBUG)
                elif lop.value == 'off':
                    logging.getLogger().setLevel(logging.INFO)
                else:
                    raise ValueError
            self.options.set_from_str(lop.option, lop.value)
        elif Planner.options.provides(lop.option) is not None:
            Planner.options.set_from_str(lop.option, lop.value)
        else:
            raise ValidatorException('SET option unknown')
        return signal_parent_commit

    def repl(self) -> None:
        def prompt(line_number: int) -> str:
            s = 'ddb> '
            if line_number > 1:
                s = f'{{: >{len(s)}}}'.format(str(line_number) + '> ')
            return s
        import readline
        print(f'''{ANSI.PROMPT}       Welcome to the      \_|_/(\__/)
      Devil's Database!      |  (. .)/ A
    Duke CompSci 516 2024   @_   <  /  ){ANSI.END}''')
        lines: list[str] = list()
        try:
            while True:
                lines.append(input(f'\001{ANSI.PROMPT}\002{prompt(len(lines)+1)}\001{ANSI.END}\002'))
                if not lines[-1].rstrip().endswith(';'): # a heuristic to detect statement boundary
                    continue
                try:
                    for parse_tree in parse_all('\n'.join(lines)):
                        r = self.request(parse_tree)
                        for s in r.pstr():
                            print(s)
                    lines.clear()
                except ParserException as e:
                    logging.error(str(e))
                    logging.info(e.__cause__)
                    lines.clear()
        except EOFError:
            print()
            print(f'{ANSI.PROMPT}Au revoir!{ANSI.END}')
            pass

    def source(self, f: TextIO) -> None:
        try:
            for parse_tree in parse_all(f.read()):
                r = self.request(parse_tree)
                for s in r.pstr():
                    print(s)
        except ParserException as e:
            logging.error(str(e))
            logging.info(e.__cause__)
        return