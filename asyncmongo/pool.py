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

from threading import Condition
import logging
from errors import TooManyConnections, ProgrammingError
from connection import Connection


class ConnectionPools(object):
    """ singleton to keep track of named connection pools """
    @classmethod
    def get_connection_pool(self, pool_id, *args, **kwargs):
        """get a connection pool, transparently creating it if it doesn't already exist

        :Parameters:
            - `pool_id`: unique id for a connection pool
        """
        assert isinstance(pool_id, (str, unicode))
        if not hasattr(self, '_pools'):
            self._pools = {}
        if pool_id not in self._pools:
            self._pools[pool_id] = ConnectionPool(*args, **kwargs)
        # logging.debug("%s: _connections = %d", pool_id, self._pools[pool_id]._connections)
        return self._pools[pool_id]
    
    @classmethod
    def close_idle_connections(self, pool_id=None):
        """close idle connections to mongo"""
        if not hasattr(self, '_pools'):
            return

        if pool_id:
            if pool_id not in self._pools:
                raise ProgrammingError("pool %r does not exist" % pool_id)
            else:
                pool = self._pools[pool_id]
                pool.close()
        else:
            for pool_id, pool in self._pools.items():
                pool.close()

class ConnectionPool(object):
    """Connection Pool to a single mongo instance.
    
    :Parameters:
      - `mincached` (optional): minimum connections to open on instantiation. 0 to open connections on first use
      - `maxcached` (optional): maximum inactive cached connections for this pool. 0 for unlimited
      - `maxconnections` (optional): maximum open connections for this pool. 0 for unlimited
      - `maxusage` (optional): number of requests allowed on a connection before it is closed. 0 for unlimited
      - `dbname`: mongo database name
      - `slave_okay` (optional): is it okay to connect directly to and perform queries on a slave instance
      - `**kwargs`: passed to `connection.Connection`
    
    """
    def __init__(self, 
                mincached=0, 
                maxcached=0, 
                maxconnections=0, 
                maxusage=0, 
                dbname=None, 
                slave_okay=False, 
                *args, **kwargs):
        assert isinstance(mincached, int)
        assert isinstance(maxcached, int)
        assert isinstance(maxconnections, int)
        assert isinstance(maxusage, int)
        assert isinstance(dbname, (str, unicode, None.__class__))
        assert isinstance(slave_okay, bool)
        if mincached and maxcached:
            assert mincached <= maxcached
        if maxconnections:
            assert maxconnections >= maxcached
            assert maxconnections >= mincached
        self._args, self._kwargs = args, kwargs
        self._maxusage = maxusage
        self._mincached = mincached
        self._maxcached = maxcached
        self._maxconnections = maxconnections
        self._idle_cache = [] # the actual connections that can be used
        self._condition = Condition()
        self._dbname = dbname
        self._slave_okay = slave_okay
        self._connections = 0

        
        # Establish an initial number of idle database connections:
        idle = [self.connection() for i in range(mincached)]
        while idle:
            self.cache(idle.pop())
    
    def new_connection(self):
        kwargs = self._kwargs
        kwargs['pool'] = self
        return Connection(*self._args, **kwargs)
    
    def connection(self):
        """ get a cached connection from the pool """
        
        self._condition.acquire()
        try:
            if (self._maxconnections and self._connections >= self._maxconnections):
                raise TooManyConnections("%d connections are already equal to the max: %d" % (self._connections, self._maxconnections))
            # connection limit not reached, get a dedicated connection
            try: # first try to get it from the idle cache
                con = self._idle_cache.pop(0)
            except IndexError: # else get a fresh connection
                con = self.new_connection()
            self._connections += 1
        finally:
            self._condition.release()
        return con

    def cache(self, con):
        """Put a dedicated connection back into the idle cache."""
        if self._maxusage and con.usage_count > self._maxusage:
            self._connections -=1
            logging.debug('dropping connection %s uses past max usage %s' % (con.usage_count, self._maxusage))
            con._close()
            return
        self._condition.acquire()
        if con in self._idle_cache:
            # called via socket close on a connection in the idle cache
            self._condition.release()
            return
        try:
            if not self._maxcached or len(self._idle_cache) < self._maxcached:
                # the idle cache is not full, so put it there
                self._idle_cache.append(con)
            else: # if the idle cache is already full,
                logging.debug('dropping connection. connection pool (%s) is full. maxcached %s' % (len(self._idle_cache), self._maxcached))
                con._close() # then close the connection
            self._condition.notify()
        finally:
            self._connections -= 1
            self._condition.release()
    
    def close(self):
        """Close all connections in the pool."""
        self._condition.acquire()
        try:
            while self._idle_cache: # close all idle connections
                con = self._idle_cache.pop(0)
                try:
                    con._close()
                except Exception:
                    pass
                self._connections -=1
            self._condition.notifyAll()
        finally:
            self._condition.release()
    

