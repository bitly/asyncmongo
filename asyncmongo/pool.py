from threading import Condition
import logging
from errors import TooManyConnections
from connection import Connection

class ConnectionPools(object):
    """ singleton to keep track of named connection pools """
    @classmethod
    def get_connection_pool(self, pool_id, *args, **kwargs):
        """get a named connection pool, transparently creating it if it doesn't already exist"""
        assert isinstance(pool_id, (str, unicode))
        if not hasattr(self, '_pools'):
            self._pools = {}
        if pool_id not in self._pools:
            self._pools[pool_id] = ConnectionPool(*args, **kwargs)
        return self._pools[pool_id]

class ConnectionPool(object):
    def __init__(self, mincached=0, maxcached=0, 
                maxconnections=0, maxusage=0, dbname=None, *args, **kwargs):
        assert isinstance(mincached, int)
        assert isinstance(maxcached, int)
        assert isinstance(maxconnections, int)
        assert isinstance(maxusage, int)
        assert isinstance(dbname, (str, unicode, None.__class__))
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
        
        # Establish an initial number of idle database connections:
        idle = [self.dedicated_connection() for i in range(mincached)]
        while idle:
            idle.pop().close()
        self._connections = 0
    
    def new_connection(self):
        kwargs = self._kwargs
        kwargs['pool'] = self
        return Connection(*self._args, **kwargs)
    
    def connection(self):
        """ get a cached connection from the pool """
        
        self._condition.acquire()
        try:
            while (self._maxconnections
                    and self._connections >= self._maxconnections):
                raise TooManyConnections("%d connections are active greater than max: %d" % (self._connections, self._maxconnections))
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
            con.close()
            return
        self._condition.acquire()
        try:
            if not self._maxcached or len(self._idle_cache) < self._maxcached:
                # the idle cache is not full, so put it there
                self._idle_cache.append(con)
            else: # if the idle cache is already full,
                con.close() # then close the connection
            self._connections -= 1
            self._condition.notify()
        finally:
            self._condition.release()
    
    def close(self):
        """Close all connections in the pool."""
        self._condition.acquire()
        try:
            while self._idle_cache: # close all idle connections
                con = self._idle_cache.pop(0)
                try:
                    con.close()
                except Exception:
                    pass
                self._connections -=1
            self._condition.notifyAll()
        finally:
            self._condition.release()
    
    # def __del__(self):
    #     """Delete the pool."""
    #     try:
    #         self.close()
    #     except Exception:
    #         pass
    

