"""
import asyncmongo
db  = asymongo.connect(host, port)

class Handler(tornado.web.RequestHandler):
        
    def get(self):
        db.history.users.find({'username': self.current_user}, limit=1, callback=self._on_response)
    
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