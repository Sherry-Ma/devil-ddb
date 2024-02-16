from typing import cast, Final
import shutil
import argparse
import logging

from .globals import ANSI
from .storage import LMDBStorageManager
from .metadata import MetadataManager

from .stats import StatsManager, TableStats, CollectionStats, NaiveStatsManager
from .transaction.baseline import LMDBTransactionManager
from .session import Session

class DatabaseManager:
    DEFAULT_DB_DIR: Final[str] = 'alps.ddb'
    """Default directory where data resides.
    """
    DEFAULT_TMP_DIR: Final[str] = 'alps-tmp.ddb'
    """Default directory for temporary files.
    """

    def __init__(self, db_dir: str, tmp_dir: str) -> None:
        self.db_dir: Final = db_dir
        self.tmp_dir: Final = tmp_dir
        shutil.rmtree(tmp_dir, ignore_errors=True)
        self.sm: Final = LMDBStorageManager(db_dir, tmp_dir)
        self.mm: Final = MetadataManager(self.sm)
        self.zm: Final = cast(StatsManager[TableStats, CollectionStats], NaiveStatsManager(self.sm, self.mm))
        self.tm: Final = LMDBTransactionManager(self.sm)
        return

def main() -> None:
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--debug', '-d', action='store_true',
                           help='debug output')
    argparser.add_argument('--inputfile', '-i', type=str,
                           help='input file')
    argparser.add_argument('--echo', '-e', action='store_true',
                           help='echo input')
    argparser.add_argument('dbdir', type=str, nargs='?', default=DatabaseManager.DEFAULT_DB_DIR,
                           help=f'database directory (defaults to {DatabaseManager.DEFAULT_DB_DIR}/)')
    argparser.add_argument('tmpdir', type=str, nargs='?', default=DatabaseManager.DEFAULT_TMP_DIR,
                           help=f'tmp directory (defaults to {DatabaseManager.DEFAULT_TMP_DIR}/)')
    args = argparser.parse_args()
    class LogFormatter(logging.Formatter):
        COLORS: Final = {
            logging.DEBUG: ANSI.DEBUG,
            logging.INFO: ANSI.INFO,
            logging.WARNING: ANSI.ERROR,
            logging.ERROR: ANSI.ERROR,
            logging.CRITICAL: ANSI.ERROR,
        }
        def format(self, record):
            if record.levelno <= logging.INFO:
                return f'{self.COLORS[record.levelno]}{record.levelname}:{ANSI.END} {super().format(record)}'
            else:
                return f'{self.COLORS[record.levelno]}{record.levelname}: {super().format(record)}{ANSI.END}'
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(LogFormatter())
    logging.getLogger().addHandler(log_handler)
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    dbm = DatabaseManager(args.dbdir, args.tmpdir)
    with Session(dbm) as s:
        if args.inputfile is None:
            s.repl()
        else:
            with open(args.inputfile) as f:
                s.source(f)
    return

if __name__ == '__main__':
    main()
