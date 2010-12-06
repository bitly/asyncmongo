import tornado.ioloop
import time
import logging

import test_shunt
import asyncmongo

TEST_TIMESTAMP = int(time.time())

def test_insert():
    test_shunt.setup()
    db = asyncmongo.Client(pool_id='testinsert', host='127.0.0.1', port=27017, dbname='test')
    
    def insert_callback(response, error):
        logging.info(response)
        assert len(response) == 1
        test_shunt.register_called('inserted')
        tornado.ioloop.IOLoop.instance().stop()
    
    db.test_users.insert({"_id" : "insert.%d" % TEST_TIMESTAMP}, callback=insert_callback)
    
    tornado.ioloop.IOLoop.instance().start()
    test_shunt.assert_called('inserted')
    
    def query_callback(response, error):
        logging.info(response)
        assert len(response) == 1
        test_shunt.register_called('retrieved')
        tornado.ioloop.IOLoop.instance().stop()
    
    db.test_users.find_one({"_id" : "insert.%d" % TEST_TIMESTAMP}, callback=query_callback)
    tornado.ioloop.IOLoop.instance().start()
    test_shunt.assert_called('retrieved')


    def delete_callback(response, error):
        logging.info(response)
        assert len(response) == 1
        test_shunt.register_called('deleted')
        tornado.ioloop.IOLoop.instance().stop()
    
    db.test_users.remove({"_id" : "insert.%d" % TEST_TIMESTAMP}, callback=delete_callback)
    tornado.ioloop.IOLoop.instance().start()
    test_shunt.assert_called('deleted')

