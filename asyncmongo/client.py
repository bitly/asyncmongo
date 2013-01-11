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
from bson.son import SON
from functools import partial

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
          - `backend': async loop backend, default = tornado
      - `**kwargs`: passed to `connection.Connection`
          - `host`: hostname or ip of mongo host
          - `port`: port to connect to
          - `slave_okay` (optional): is it okay to connect directly to and perform queries on a slave instance
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

    def collection_names(self, callback):
        """Get a list of all the collection names in selected database"""
        callback = partial(self._collection_names_result, callback)
        self["system.namespaces"].find(_must_use_master=True, callback=callback)

    def _collection_names_result(self, callback, results, error=None):
        """callback to for collection names query, filters out collection names"""
        names = [r['name'] for r in results if r['name'].count('.') == 1]
        assert error == None, repr(error)
        strip = len(self._pool._dbname) + 1
        callback([name[strip:] for name in names])

    def command(self, command, value=1, callback=None,
                check=True, allowable_errors=[], **kwargs):
        """Issue a MongoDB command.

        Send command `command` to the database and return the
        response. If `command` is an instance of :class:`basestring`
        then the command {`command`: `value`} will be sent. Otherwise,
        `command` must be an instance of :class:`dict` and will be
        sent as is.

        Any additional keyword arguments will be added to the final
        command document before it is sent.

        For example, a command like ``{buildinfo: 1}`` can be sent
        using:

        >>> db.command("buildinfo")

        For a command where the value matters, like ``{collstats:
        collection_name}`` we can do:

        >>> db.command("collstats", collection_name)

        For commands that take additional arguments we can use
        kwargs. So ``{filemd5: object_id, root: file_root}`` becomes:

        >>> db.command("filemd5", object_id, root=file_root)

        :Parameters:
          - `command`: document representing the command to be issued,
            or the name of the command (for simple commands only).

            .. note:: the order of keys in the `command` document is
               significant (the "verb" must come first), so commands
               which require multiple keys (e.g. `findandmodify`)
               should use an instance of :class:`~bson.son.SON` or
               a string and kwargs instead of a Python `dict`.

          - `value` (optional): value to use for the command verb when
            `command` is passed as a string
          - `**kwargs` (optional): additional keyword arguments will
            be added to the command document before it is sent

        .. mongodoc:: commands
        """

        if isinstance(command, basestring):
            command = SON([(command, value)])

        command.update(kwargs)

        self.connection("$cmd").find_one(command,callback=callback,
                                       _must_use_master=True,
                                       _is_command=True)
