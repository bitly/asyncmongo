import tornado.ioloop
import logging
import time

import test_shunt
import asyncmongo
from asyncmongo.errors import DataError

TEST_TIMESTAMP = int(time.time())

class ConnectionTest(test_shunt.MongoTest):
    def test_getitem(self):
        db = asyncmongo.Client(pool_id='test_query', host='127.0.0.1', port=27018, dbname='test', mincached=3)
        self.assert_(
            repr(db['foo']) == repr(db.foo),
            "dict-style access of a collection should be same as property access"
        )

    def test_connection(self):
        db = asyncmongo.Client(pool_id='test_query', host='127.0.0.1', port=27018, dbname='test', mincached=3)
        for connection_name in [
            '.',
            '..',
            '.foo',
            'foo.',
            '.foo.',
            'foo\x00'
            '\x00foo'
        ]:
            self.assertRaises(
                DataError,
                lambda: db.connection(connection_name)
            )

    def test_query(self):
        logging.info('in test_query')
        test_shunt.setup()
        db = asyncmongo.Client(pool_id='test_query', host='127.0.0.1', port=27018, dbname='test', mincached=3)
        
        def insert_callback(response, error):
            tornado.ioloop.IOLoop.instance().stop()
            logging.info(response)
            assert len(response) == 1
            test_shunt.register_called('inserted')

        db.test_users.insert({"_id" : "test_connection.%d" % TEST_TIMESTAMP}, safe=True, callback=insert_callback)
        
        tornado.ioloop.IOLoop.instance().start()
        test_shunt.assert_called('inserted')
        
        def callback(response, error):
            tornado.ioloop.IOLoop.instance().stop()
            assert len(response) == 1
            test_shunt.register_called('got_record')

        db.test_users.find({}, limit=1, callback=callback)
        
        tornado.ioloop.IOLoop.instance().start()
        test_shunt.assert_called("got_record")
