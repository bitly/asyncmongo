import tornado.ioloop
import logging
import time

import test_shunt
import asyncmongo
TEST_TIMESTAMP = int(time.time())

def test_pooled_db():
    """
    This tests simply verifies that we can grab two different connections from the pool
    and use them independantly.
    """
    print asyncmongo.__file__
    test_shunt.setup()
    pool = asyncmongo.PooledDB(asyncmongo, maxconnections=5, host='127.0.0.1', port=27017, dbname='test')

    db = pool.connection()
    cursor = db.cursor('test_users')
    
    def insert_callback(response, error):
        logging.info(response)
        assert len(response) == 1
        test_shunt.register_called('inserted')
        tornado.ioloop.IOLoop.instance().stop()
    
    cursor.insert({"_id" : "record_test.%d" % TEST_TIMESTAMP}, safe=True, callback=insert_callback)
    
    tornado.ioloop.IOLoop.instance().start()
    test_shunt.assert_called('inserted')
    
    
    db2 = pool.connection()
    cursor2 = db2.cursor('test_users')

    def pool_callback(response, error):
        assert len(response) == 1
        test_shunt.register_called('pool1')
        if test_shunt.is_called('pool2'):
            tornado.ioloop.IOLoop.instance().stop()

    def pool_callback2(response, error):
        assert len(response) == 1
        test_shunt.register_called('pool2')
        
        if test_shunt.is_called('pool1'):
            # don't expect 2 finishes second
            tornado.ioloop.IOLoop.instance().stop()
    
    cursor.find({}, limit=1, callback=pool_callback)
    cursor2.find({}, limit=1, callback=pool_callback2)
    
    tornado.ioloop.IOLoop.instance().start()
    test_shunt.assert_called('pool1')
    test_shunt.assert_called('pool2')
