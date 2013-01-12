import tornado.ioloop
import time
import logging

import test_shunt
import asyncmongo

TEST_TIMESTAMP = int(time.time())

class DuplicateInsertTest(test_shunt.MongoTest):
    def test_duplicate_insert(self):
        test_shunt.setup()
        db = asyncmongo.Client(pool_id='dup_insert', host='127.0.0.1', port=27018, dbname='test')
        
        def insert_callback(response, error):
            tornado.ioloop.IOLoop.instance().stop()
            logging.info(response)
            assert len(response) == 1
            test_shunt.register_called('inserted')

        db.test_users.insert({"_id" : "duplicate_insert.%d" % TEST_TIMESTAMP}, callback=insert_callback)
        
        tornado.ioloop.IOLoop.instance().start()
        test_shunt.assert_called('inserted')
        
        def duplicate_callback(response, error):
            tornado.ioloop.IOLoop.instance().stop()
            logging.info(response)
            if error:
                test_shunt.register_called('dupe')

        db.test_users.insert({"_id" : "duplicate_insert.%d" % TEST_TIMESTAMP}, callback=duplicate_callback)
        
        tornado.ioloop.IOLoop.instance().start()
        test_shunt.assert_called('dupe')

