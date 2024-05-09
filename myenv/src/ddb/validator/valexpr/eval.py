"""This module defines the functions that can be used when compiling expression evaluation code.
Any module that needs to evaluate such compile code should do ``import *`` on this module.
"""

import re
from math import sqrt
from dateutil.parser import parse as str_to_datetime

def regexp_match(s: str, pattern: str):
    return re.match(pattern, s) is not None
