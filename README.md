asyncmongo
==========

AsyncMongo is an asynchronous library for accessing mongo 
which is built on the tornado ioloop.

Installation
------------

Installing: `pip install asyncmongo`

Installing form github: `pip install -e git://github.com/bitly/asyncmongo.git#egg=asyncmongo`

Installing from source: `git clone git://github.com/bitly/asyncmongo.git; cd asyncmongo; python setup.py install`

Usage
-----

    import asyncmongo
    import tornado.web
    
    class Handler(tornado.web.RequestHandler):
        @property
        def db(self):
            if not hasattr(self, '_db'):
                self._db = asyncmongo.Client(pool_id='mydb', host='127.0.0.1', port=27017, maxcached=10, maxconnections=50, dbname='test')
            return self._db
    
        @tornado.web.asynchronous
        def get(self):
            self.db.users.find({'username': self.current_user}, limit=1, callback=self._on_response)
            # or
            # conn = self.db.connection(collectionname="...", dbname="...")
            # conn.find(..., callback=self._on_response)
    
        def _on_response(self, response, error):
            if error:
                raise tornado.web.HTTPError(500)
            self.render('template', full_name=respose['full_name'])

About
-----

Features not supported: some features from pymongo are not currently implemented. i.e.: directly 
interfacing with indexes, dropping collections, and retrieving results in batches instead of all at once. 
(asyncmongo's nature means that no calls are blocking regardless of the number of results you are retrieving)

Requirements
------------
The following two python libraries are required

* [pymongo](http://github.com/mongodb/mongo-python-driver) version 1.9+ for bson library
* [tornado](http://github.com/facebook/tornado)

Issues
------

Please report any issues via [github issues](https://github.com/bitly/asyncmongo/issues)
