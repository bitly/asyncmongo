"""
import asyncmongo
from DBUtils import PooledDB
db_pool = PooledDB.PooledDB(asyncmongo, host='127.0.0.1', port=27107, dbname='test', maxconnections=50)

class Handler(tornado.web.RequestHandler):
    @property
    def db(self):
        if not hasattr(self, '_db'):
            self._db = db_pool.dedicated_connection()
        return self._db
    
    def get(self):
        self.db.history.users.find({'username': self.current_user}, limit=1, callback=self._on_response)
    
    def _on_response(self, response):
        self.render('template', full_name=respose['full_name'])

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


from connection import Connection
def connect(*args, **kwargs):
    return Connection(*args, **kwargs)