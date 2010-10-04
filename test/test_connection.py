import test_shunt

import sys
sys.path.append("")
import asyncmongo
import tornado.ioloop

called = False
def test_query():
    db = asyncmongo.connect('127.0.0.1', 27017, dbname='history')
    
    def callback(response):
        global called
        assert len(response) == 1
        called = True
        tornado.ioloop.IOLoop.instance().stop()
    
    db.users.find({}, limit=1, callback=callback)
    
    tornado.ioloop.IOLoop.instance().start()
    assert called == True
