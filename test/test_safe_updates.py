import tornado.ioloop
import time
import logging

import test_shunt
import asyncmongo

TEST_TIMESTAMP = int(time.time())

class SafeUpdatesTest(test_shunt.MongoTest):
    def test_update_safe(self):
        test_shunt.setup()
        db = asyncmongo.Client(pool_id='testinsert', host='127.0.0.1', port=27018, dbname='test', maxconnections=2)
        
        def update_callback(response, error):
            tornado.ioloop.IOLoop.instance().stop()
            logging.info(response)
            assert len(response) == 1
            test_shunt.register_called('update')

        # all of these should be called, but only one should have a callback
        # we also are checking that connections in the pool never increases >1 with max_connections=2
        # this is because connections for safe=False calls get put back in the pool immediated
        db.test_stats.update({"_id" : TEST_TIMESTAMP}, {'$inc' : {'test_count' : 1}}, safe=False, upsert=True)
        db.test_stats.update({"_id" : TEST_TIMESTAMP}, {'$inc' : {'test_count' : 1}}, safe=False, upsert=True)
        db.test_stats.update({"_id" : TEST_TIMESTAMP}, {'$inc' : {'test_count' : 1}}, safe=False, upsert=True)
        db.test_stats.update({"_id" : TEST_TIMESTAMP}, {'$inc' : {'test_count' : 1}}, safe=False, upsert=True)
        db.test_stats.update({"_id" : TEST_TIMESTAMP}, {'$inc' : {'test_count' : 1}}, upsert=True, callback=update_callback)
        
        tornado.ioloop.IOLoop.instance().start()
        test_shunt.assert_called('update')
        
        def query_callback(response, error):
            tornado.ioloop.IOLoop.instance().stop()
            logging.info(response)
            assert isinstance(response, dict)
            assert response['_id'] == TEST_TIMESTAMP
            assert response['test_count'] == 5
            test_shunt.register_called('retrieved')

        db.test_stats.find_one({"_id" : TEST_TIMESTAMP}, callback=query_callback)
        tornado.ioloop.IOLoop.instance().start()
        test_shunt.assert_called('retrieved')
