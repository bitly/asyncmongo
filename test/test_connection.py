import tornado.ioloop
import logging
import time

import test_shunt
import asyncmongo

TEST_TIMESTAMP = int(time.time())

def test_query():
    test_shunt.setup()
    db = asyncmongo.Client(pool_id='test_query', host='127.0.0.1', port=27017, dbname='test', mincached=3)

    def insert_callback(response, error):
        logging.info(response)
        assert len(response) == 1
        test_shunt.register_called('inserted')
        tornado.ioloop.IOLoop.instance().stop()
    
    db.test_users.insert({"_id" : "test_connection.%d" % TEST_TIMESTAMP}, safe=True, callback=insert_callback)
    
    tornado.ioloop.IOLoop.instance().start()
    test_shunt.assert_called('inserted')
    
    def callback(response, error):
        assert len(response) == 1
        test_shunt.register_called('got_record')
        tornado.ioloop.IOLoop.instance().stop()
    
    db.test_users.find({}, limit=1, callback=callback)
    
    tornado.ioloop.IOLoop.instance().start()
    test_shunt.assert_called("got_record")
