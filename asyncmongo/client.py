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

from errors import DataError
from pool import ConnectionPools
from cursor import Cursor

class Client(object):
    """
    Client connection to represent a remote database.
    
    Internally Client maintains a pool of connections that will live beyond the life of this object.
    
    Usage:
        db = asyncmongo.Client(host, port, dbname).connection(collectionname)
        db.find({...}, callback=...)
        
    """
    def __init__(self, pool_id=None, **kwargs):
        self._pool = ConnectionPools.get_connection_pool(pool_id, **kwargs)
    
    def __getattr__(self, name):
        """Get a collection by name.

        :Parameters:
          - `name`: the name of the collection
        """
        return self.connection(name)

    def __getitem__(self, name):
        """Get a collection by name.
        :Parameters:
          - `name`: the name of the collection to get
        """
        return self.connection(name)
    
    def connection(self, collectionname, dbname=None):
        """Get a cursor to a collection by name.

        raises `DataError` on names with unallowable characters.

        :Parameters:
          - `collectionname`: the name of the collection
          - `dbname`: (optional) overide the default db for a connection
          
        """
        if not collectionname or ".." in collectionname:
            raise DataError("collection names cannot be empty")
        if "$" in collectionname and not (collectionname.startswith("oplog.$main") or
                                collectionname.startswith("$cmd")):
            raise DataError("collection names must not "
                              "contain '$': %r" % collectionname)
        if collectionname.startswith(".") or collectionname.endswith("."):
            raise DataError("collecion names must not start "
                            "or end with '.': %r" % collectionname)
        if "\x00" in collectionname:
            raise DataError("collection names must not contain the "
                              "null character")
        return Cursor(dbname or self._pool._dbname, collectionname, self._pool)
