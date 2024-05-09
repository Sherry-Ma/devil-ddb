"""Constants globally visible to the entire system.
"""
from typing import Final

BLOCK_SIZE: Final[int] = 4028
"""Size of a memory/disk block (a unit of transfer), in bytes.
"""

MAX_DB_SIZE: Final[int] = 1000000000000
"""Max size of a database.
"""

MAX_FILES: Final[int] = 10000
"""Max number of files/tables.
For example, it may place a practical limit on how many runs an external merge sort may produce.
"""

DEFAULT_BNLJ_BUFFER_SIZE: Final[int] = 10
"""Default number of blocks used by block-based nested-loop join.
"""

DEFAULT_SORT_BUFFER_SIZE: Final[int] = 10
"""Default number of blocks used by sorting.
"""

DEFAULT_SORT_LAST_BUFFER_SIZE: Final = 5
"""Default number of blocks used by sorting, if the sort is supplying output to a sort-merge join.
"""

DEFAULT_HASH_BUFFER_SIZE: Final[int] = 10
"""Default number of blocks used by hashing.
"""

DEFAULT_HASH_MAX_DEPTH: Final[int] = 3
"""Default cap on the number partitioning passes for hashing.
Note that the number of partitions grows by a factor of roughly ``DEFAULT_HASH_BUFFER_SIZE``
with each partitioning passes.
In the case of data skew or (unlikely) hash collision, this cap will prevent futile partitioning passes
creating too many partition files.
TODO: A more graceful way of handling skew is needed instead.
"""

class ANSI:
    """ANSI formatting escape codes.
    """
    PROMPT = '\033[94m'

    EMPH = '\033[103m'
    DEMPH = '\033[2m'
    H1 = '\033[103m'
    H2 = '\033[93m'
    UNDERLINE = "\033[4m"
    END = '\033[0m'

    DEBUG = '\033[32m'
    INFO = '\033[36m'
    ERROR = '\033[101m'
