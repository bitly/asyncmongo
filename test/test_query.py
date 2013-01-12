import tornado.ioloop
import logging
import time

import test_shunt
import asyncmongo


class QueryTest(test_shunt.MongoTest, test_shunt.SynchronousMongoTest):
    mongod_options = [('--port', '27018')]

    def setUp(self):
        super(QueryTest, self).setUp()
        self.pymongo_conn.test.foo.insert([{'i': i} for i in xrange(200)])

    def test_query(self):
        db = asyncmongo.Client(pool_id='test_query', host='127.0.0.1', port=int(self.mongod_options[0][1]), dbname='test', mincached=3)

        def noop_callback(response, error):
            logging.info(response)
            loop = tornado.ioloop.IOLoop.instance()
            # delay the stop so kill cursor has time on the ioloop to get pushed through to mongo
            loop.add_timeout(time.time() + .1, loop.stop)

        before = self.get_open_cursors()

        # run 2 queries
        db.foo.find({}, callback=noop_callback)
        tornado.ioloop.IOLoop.instance().start()
        db.foo.find({}, callback=noop_callback)
        tornado.ioloop.IOLoop.instance().start()
        
        # check cursors
        after = self.get_open_cursors()
        assert before == after, "%d cursors left open (should be 0)" % (after - before)

if __name__ == '__main__':
    import unittest
    unittest.main()
