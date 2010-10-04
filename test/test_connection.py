import test_shunt

import sys
sys.path.append("")
import asyncmongo
import tornado.ioloop

def test_query():
    db = asyncmongo.connect('127.0.0.1', 27017, dbname='history')
    
    called = False

    def callback(response):
        global called
        assert response['cursor_id'] == 0
        assert len(response['data']) == 1
        assert response['number_returned'] == 1
        assert response['starting_from'] == 0
        
        called = True
        tornado.ioloop.IOLoop.instance().stop()
    
    db.users.find({}, limit=1, callback=callback)
    
    tornado.ioloop.IOLoop.instance().start()
    assert called == True