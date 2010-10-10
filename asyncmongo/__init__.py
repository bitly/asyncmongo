"""
asyncmongo is a library for accessing tornado built built upon the tornado io loop
"""
version = "0.0.1"

ASCENDING = 1
"""Ascending sort order."""
DESCENDING = -1
"""Descending sort order."""
GEO2D = "2d"
"""Index specifier for a 2-dimensional `geospatial index`"""

# DBAPI v2 variables
apilevel = 2
threadsafety = 1 # share the module, not connections

from errors import Warning, Error, InterfaceError, DatabaseError, DataError, OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError

from connection import Connection
def connect(*args, **kwargs):
    return Connection(*args, **kwargs)

from pool import PooledDB