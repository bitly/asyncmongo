import tornado.ioloop
import logging
import time
from asyncmongo.errors import TooManyConnections

import test_shunt
import asyncmongo
TEST_TIMESTAMP = int(time.time())

class PooledDBTest(test_shunt.MongoTest):
    def test_pooled_db(self):
        """
        This tests simply verifies that we can grab two different connections from the pool
        and use them independantly.
        """
        print asyncmongo.__file__
        test_shunt.setup()
        client = asyncmongo.Client('id1', maxconnections=5, host='127.0.0.1', port=27018, dbname='test')
        test_users_collection = client.connection('test_users')
        
        def insert_callback(response, error):
            tornado.ioloop.IOLoop.instance().stop()
            logging.info(response)
            assert len(response) == 1
            test_shunt.register_called('inserted')

        test_users_collection.insert({"_id" : "record_test.%d" % TEST_TIMESTAMP}, safe=True, callback=insert_callback)
        
        tornado.ioloop.IOLoop.instance().start()
        test_shunt.assert_called('inserted')
        
        def pool_callback(response, error):
            if test_shunt.is_called('pool2'):
                tornado.ioloop.IOLoop.instance().stop()
            assert len(response) == 1
            test_shunt.register_called('pool1')

        def pool_callback2(response, error):
            if test_shunt.is_called('pool1'):
                # don't expect 2 finishes second
                tornado.ioloop.IOLoop.instance().stop()
            assert len(response) == 1
            test_shunt.register_called('pool2')

        test_users_collection.find({}, limit=1, callback=pool_callback)
        test_users_collection.find({}, limit=1, callback=pool_callback2)
        
        tornado.ioloop.IOLoop.instance().start()
        test_shunt.assert_called('pool1')
        test_shunt.assert_called('pool2')

    def too_many_connections(self):
        clients = [
            asyncmongo.Client('id2', maxconnections=2, host='127.0.0.1', port=27018, dbname='test')
            for i in range(3)
        ]

        def callback(response, error):
            pass

        for client in clients[:2]:
            client.connection('foo').find({}, callback=callback)

        self.assertRaises(
            TooManyConnections,
            lambda: clients[2].connection('foo').find({}, callback=callback)
        )

