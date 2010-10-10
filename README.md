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
    import tornado.web
    db_pool = asyncmongo.PooledDB(asyncmongo, host='127.0.0.1', port=27107, dbname='test', maxconnections=50)

    class Handler(tornado.web.RequestHandler):
        @property
        def db(self):
            if not hasattr(self, '_db'):
                self._db = db_pool.connection()
            return self._db
    
        @tornado.web.asynchronous
        def get(self):
            cursor = self.db.cursor("users_collection")
            cursor.users.find({'username': self.current_user}, limit=1, callback=self._on_response)
    
        def _on_response(self, response, error):
            if error:
                raise tornado.web.HTTPError(500)
            self.render('template', full_name=respose['full_name'])

About
-----

Features not supported: some features from pymongo are not currently implemented: namely directly interfacing with indexes, dropping collections, and retrieving results in batches instead of all at once. (asyncmongo's nature means that no calls are blocking regardless of the number of results you are retrieving)

Requirements
------------
The following two python libraries are required

* [pymongo](http://github.com/mongodb/mongo-python-driver) version 1.9+ for bson library
* [tornado](http://github.com/facebook/tornado)

