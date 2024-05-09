"""Utility functions that serialize/deserialize Python objects for persistent storage.

TODO: The implementation of row storage is based on ``pickle``.
More efficient implementation may be possible.

NOTE: Currently, we pack/unpack integers using just 4 bytes, if they are stored as keys.
However, Python's ints are actually much, much wider.
There will be a size difference when an int column is serialized (pickled) as part of a row,
or when it's used in memory.
"""
import struct
import pickle

from .interface import StorageMangerException

def pack_int(i: int) -> bytes:
    """Serialize a signed integer into 4 bytes in big-endian format,
    such that byte order reflects numeric order.
    """
    # add offset so min negative -> all 0's, and max positive -> all 1's
    return struct.pack('>I', i + 2147483648)

def unpack_int(bytes: bytes) -> int:
    """Deserialize 4 bytes into a signed integer in a way that is consistent with :meth:.pack_int; i.e.:
    ``unpack_int(pack_int(i)) == i``.
    """
    # subtract offset so min negative <- all 0's, and max positive <- all 1's
    return struct.unpack('>I', bytes)[0] - 2147483648

def pack_str(s: str) -> bytes:
    """Serialize a string into bytes, using the default encoding."""
    return s.encode()

def unpack_str(bytes: bytes) -> str:
    """Deserialize bytes into a string in a way that is consistent with :meth:.pack_str; i.e.:
    ``unpack_str(pack_str(s)) == s``.
    """
    return bytes.decode()

def pack_row(row: tuple) -> bytes:
    """Serialize a tuple into bytes; there is no guarantee that the byte order reflects the natural row order."""
    return pickle.dumps(row)

def unpack_row(bytes: bytes) -> tuple:
    """Deserialize bytes into a tuple in a way that is consistent with :meth:.pack_row; i.e.:
    ``unpack_row(pack_row(t)) == t``.
    """
    obj = pickle.loads(bytes)
    if type(obj) is not tuple:
        raise StorageMangerException(f'unpacked object is not a row: {str(obj)}')
    else:
        return obj
