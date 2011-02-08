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
    
    :Parameters:
      - `pool_id`: unique id for this connection pool
      - `**kwargs`: passed to `pool.ConnectionPool`
          - `mincached` (optional): minimum connections to open on instantiation. 0 to open connections on first use
          - `maxcached` (optional): maximum inactive cached connections for this pool. 0 for unlimited
          - `maxconnections` (optional): maximum open connections for this pool. 0 for unlimited
          - `maxusage` (optional): number of requests allowed on a connection before it is closed. 0 for unlimited
          - `dbname`: mongo database name
      - `**kwargs`: passed to `connection.Connection`
          - `host`: hostname or ip of mongo host
          - `port`: port to connect to
          - `slave_ok` (optional): is it okay to connect directly to and perform queries on a slave instance
          - `autoreconnect` (optional): auto reconnect on interface errors
    
    @returns a `Client` instance that wraps a `pool.ConnectionPool`
    
    Usage:
        >>> db = asyncmongo.Client(pool_id, host=host, port=port, dbname=dbname)
        >>> db.collectionname.find({...}, callback=...)
        
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
