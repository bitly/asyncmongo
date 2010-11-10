"""
asyncmongo is a library for accessing tornado built built upon the tornado io loop
"""
version = "0.0.2"

ASCENDING = 1
"""Ascending sort order."""
DESCENDING = -1
"""Descending sort order."""
GEO2D = "2d"
"""Index specifier for a 2-dimensional `geospatial index`"""

from errors import Warning, Error, InterfaceError, DatabaseError, DataError, OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError

from client import Client
