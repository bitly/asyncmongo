import tornado.ioloop

import test_shunt
import asyncmongo


class CommandTest(
    test_shunt.MongoTest,
    test_shunt.SynchronousMongoTest,
):
    mongod_options = [('--port', '27018')]

    def setUp(self):
        super(CommandTest, self).setUp()
        self.pymongo_conn.test.foo.insert({'_id': 1})

    def test_find_and_modify(self):
        db = asyncmongo.Client(pool_id='test_query', host='127.0.0.1', port=int(self.mongod_options[0][1]), dbname='test', mincached=3)

        results = []

        def callback(response, error):
            tornado.ioloop.IOLoop.instance().stop()
            self.assert_(error is None)
            results.append(response['value'])

        before = self.get_open_cursors()

        # First findAndModify creates doc with i: 2 and s: 'a'
        db.command('findAndModify', 'foo',
            callback=callback,
            query={'_id': 2},
            update={'$set': {'s': 'a'}},
            upsert=True,
            new=True,
        )

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(
            {'_id': 2, 's': 'a'},
            results[0]
        )

        # Second findAndModify updates doc with i: 2, sets s to 'b'
        db.command('findAndModify', 'foo',
            callback=callback,
            query={'_id': 2},
            update={'$set': {'s': 'b'}},
            upsert=True,
            new=True,
        )

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(
            {'_id': 2, 's': 'b'},
            results[1]
        )

        # check cursors
        after = self.get_open_cursors()
        assert before == after, "%d cursors left open (should be 0)" % (after - before)

if __name__ == '__main__':
    import unittest
    unittest.main()
