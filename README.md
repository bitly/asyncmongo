asyncmongo
==========

AsyncMongo is an asynchronous library for accessing mongo 
which is built on the tornado ioloop.

Installation
------------

	$ python setup.py install

Usage
-----

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

Requirements
------------
The following two python libraries are required

* [pymongo](http://github.com/mongodb/mongo-python-driver) version 1.9+ for bson library
* [tornado](http://github.com/facebook/tornado)

