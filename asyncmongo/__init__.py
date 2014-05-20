#!/bin/env python
# 
# Copyright 2010 bit.ly
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""
AsyncMongo is an asynchronous library for accessing mongo 
http://github.com/bitly/asyncmongo
"""
try:
    import bson
except ImportError:
    raise ImportError("bson library not installed. Install pymongo >= 1.9 https://github.com/mongodb/mongo-python-driver")

# also update in setup.py
version = "1.3"
version_info = (1, 3)

ASCENDING = 1
"""Ascending sort order."""
DESCENDING = -1
"""Descending sort order."""
GEO2D = "2d"
"""Index specifier for a 2-dimensional `geospatial index`"""

from errors import (Error, InterfaceError, AuthenticationError, DatabaseError, RSConnectionError,
                    DataError, IntegrityError, ProgrammingError, NotSupportedError)

from client import Client
