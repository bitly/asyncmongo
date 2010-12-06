import tornado.ioloop
import time
import logging

import test_shunt
import asyncmongo

TEST_TIMESTAMP = int(time.time())

def test_update_safe():
    test_shunt.setup()
    db = asyncmongo.Client(pool_id='testinsert', host='127.0.0.1', port=27017, dbname='test')
    
    def update_callback(response, error):
        logging.info(response)
        assert len(response) == 1
        test_shunt.register_called('update')
        tornado.ioloop.IOLoop.instance().stop()
    
    # both of these should be called, but only one should have a callback
    db.test_stats.update({"_id" : TEST_TIMESTAMP}, {'$inc' : {'test_count' : 1}}, safe=False, upsert=True)
    db.test_stats.update({"_id" : TEST_TIMESTAMP}, {'$inc' : {'test_count' : 1}}, upsert=True, callback=update_callback)
    
    tornado.ioloop.IOLoop.instance().start()
    test_shunt.assert_called('update')
    
    def query_callback(response, error):
        logging.info(response)
        assert isinstance(response, dict)
        assert response['_id'] == TEST_TIMESTAMP
        assert response['test_count'] == 2
        test_shunt.register_called('retrieved')
        tornado.ioloop.IOLoop.instance().stop()
    
    db.test_stats.find_one({"_id" : TEST_TIMESTAMP}, callback=query_callback)
    tornado.ioloop.IOLoop.instance().start()
    test_shunt.assert_called('retrieved')
