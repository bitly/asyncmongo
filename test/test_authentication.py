import tornado.ioloop
import time
import logging
import subprocess

import test_shunt
import asyncmongo

TEST_TIMESTAMP = int(time.time())

class AuthenticationTest(test_shunt.MongoTest):
    def setUp(self):
        super(AuthenticationTest, self).setUp()
        logging.info('creating user')
        pipe = subprocess.Popen('''echo -e 'use test;\n db.addUser("testuser", "testpass");\n exit;' | mongo --port 27018 --host 127.0.0.1''', shell=True)
        pipe.wait()
        
    def test_authentication(self):
        try:
            test_shunt.setup()
            db = asyncmongo.Client(pool_id='testauth', host='127.0.0.1', port=27018, dbname='test', dbuser='testuser',
                                   dbpass='testpass', maxconnections=2)
        
            def update_callback(response, error):
                logging.info("UPDATE:")
                tornado.ioloop.IOLoop.instance().stop()
                logging.info(response)
                assert len(response) == 1
                test_shunt.register_called('update')

            db.test_stats.update({"_id" : TEST_TIMESTAMP}, {'$inc' : {'test_count' : 1}}, upsert=True,
                                 callback=update_callback)

            tornado.ioloop.IOLoop.instance().start()
            test_shunt.assert_called('update')

            def query_callback(response, error):
                tornado.ioloop.IOLoop.instance().stop()
                logging.info(response)
                logging.info(error)
                assert error is None
                assert isinstance(response, dict)
                assert response['_id'] == TEST_TIMESTAMP
                assert response['test_count'] == 1
                test_shunt.register_called('retrieved')

            db.test_stats.find_one({"_id" : TEST_TIMESTAMP}, callback=query_callback)
            tornado.ioloop.IOLoop.instance().start()
            test_shunt.assert_called('retrieved')
        except:
            tornado.ioloop.IOLoop.instance().stop()
            raise

    def test_failed_auth(self):
        try:
            test_shunt.setup()
            db = asyncmongo.Client(pool_id='testauth_f', host='127.0.0.1', port=27018, dbname='test', dbuser='testuser',
                                   dbpass='wrong', maxconnections=2)

            def query_callback(response, error):
                tornado.ioloop.IOLoop.instance().stop()
                logging.info(response)
                logging.info(error)
                assert isinstance(error, asyncmongo.AuthenticationError)
                assert response is None
                test_shunt.register_called('auth_failed')

            db.test_stats.find_one({"_id" : TEST_TIMESTAMP}, callback=query_callback)
            tornado.ioloop.IOLoop.instance().start()
            test_shunt.assert_called('auth_failed')
        except:
            tornado.ioloop.IOLoop.instance().stop()
            raise
