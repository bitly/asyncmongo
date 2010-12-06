import tornado.ioloop
import time
import logging

import test_shunt
import asyncmongo

TEST_TIMESTAMP = int(time.time())

def test_duplicate_insert():
    test_shunt.setup()
    db = asyncmongo.Client(pool_id='dup_insert', host='127.0.0.1', port=27017, dbname='test')
    
    def insert_callback(response, error):
        logging.info(response)
        assert len(response) == 1
        test_shunt.register_called('inserted')
        tornado.ioloop.IOLoop.instance().stop()
    
    db.test_users.insert({"_id" : "duplicate_insert.%d" % TEST_TIMESTAMP}, callback=insert_callback)
    
    tornado.ioloop.IOLoop.instance().start()
    test_shunt.assert_called('inserted')

    def duplicate_callback(response, error):
        logging.info(response)
        if error:
            test_shunt.register_called('dupe')
        tornado.ioloop.IOLoop.instance().stop()
    
    db.test_users.insert({"_id" : "duplicate_insert.%d" % TEST_TIMESTAMP}, callback=duplicate_callback)
    
    tornado.ioloop.IOLoop.instance().start()
    test_shunt.assert_called('dupe')
    

