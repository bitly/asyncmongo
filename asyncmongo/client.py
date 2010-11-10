from errors import DataError
from pool import ConnectionPools
from cursor import Cursor

class Client(object):
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
