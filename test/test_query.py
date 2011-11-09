import tornado.ioloop
import logging
import time

import test_shunt
import asyncmongo
import pymongo


class QueryTest(test_shunt.MongoTest):
    mongod_options = [('--port', '27999')]

    @property
    def pymongo_conn(self):
        if not hasattr(self, '_pymongo_conn'):
            self._pymongo_conn = pymongo.Connection(port=int(self.mongod_options[0][1]))
        return self._pymongo_conn

    def get_open_cursors(self):
        output = self.pymongo_conn.admin.command('serverStatus')
        return output.get('cursors', {}).get('totalOpen')

    def setUp(self):
        super(QueryTest, self).setUp()
        self.pymongo_conn.test.foo.insert([{'i': i} for i in xrange(200)])

    def test_query(self):
        db = asyncmongo.Client(pool_id='test_query', host='127.0.0.1', port=27999, dbname='test', mincached=3)

        def noop_callback(response, error):
            tornado.ioloop.IOLoop.instance().stop()

        before = self.get_open_cursors()
        db.foo.find(limit=20, callback=noop_callback)
        tornado.ioloop.IOLoop.instance().start()
        after = self.get_open_cursors()
        self.assertEquals(before, after, "%d cursors left open (should be 0)" % (after - before))

if __name__ == '__main__':
    import unittest
    unittest.main()
