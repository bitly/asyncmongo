"""
AsyncMongo is an asynchronous library for accessing mongo 
which is built on the tornado ioloop.

http://github.com/bitly/asyncmongo
"""
version = "0.0.4"

ASCENDING = 1
"""Ascending sort order."""
DESCENDING = -1
"""Descending sort order."""
GEO2D = "2d"
"""Index specifier for a 2-dimensional `geospatial index`"""

from errors import Error, InterfaceError, DatabaseError, DataError, IntegrityError, ProgrammingError, NotSupportedError

from client import Client
