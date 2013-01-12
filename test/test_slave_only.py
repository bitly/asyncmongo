import tornado.ioloop
import time
import logging

import test_shunt
import asyncmongo

TEST_TIMESTAMP = int(time.time())

class SlaveOnlyTest(test_shunt.MongoTest):
    mongod_options = [
        ('--port', '27018', '--master'),
        ('--port', '27019', '--slave', '--source', '127.0.0.1:27018'), 
    ]
    def test_query_slave(self):
        try:
            test_shunt.setup()
            masterdb = asyncmongo.Client(pool_id='testquerymaster', host='127.0.0.1', port=27018, dbname='test', maxconnections=2)
            slavedb = asyncmongo.Client(pool_id='testqueryslave', host='127.0.0.1', port=27019, dbname='test', maxconnections=2, slave_okay=True)
            logging.debug('waiting for replication to start (sleeping 4 seconds)')
            time.sleep(4)
        
            def update_callback(response, error):
                tornado.ioloop.IOLoop.instance().stop()
                logging.info(response)
                assert len(response) == 1
                test_shunt.register_called('update')

            masterdb.test_stats.update({"_id" : TEST_TIMESTAMP}, {'$inc' : {'test_count' : 1}}, upsert=True, callback=update_callback)
        
            tornado.ioloop.IOLoop.instance().start()
            test_shunt.assert_called('update')

            # wait for the insert to get to the slave
            time.sleep(2.5)
        
            def query_callback(response, error):
                tornado.ioloop.IOLoop.instance().stop()
                logging.info(response)
                logging.info(error)
                assert error is None
                assert isinstance(response, dict)
                assert response['_id'] == TEST_TIMESTAMP
                assert response['test_count'] == 1
                test_shunt.register_called('retrieved')

            slavedb.test_stats.find_one({"_id" : TEST_TIMESTAMP}, callback=query_callback)
            tornado.ioloop.IOLoop.instance().start()
            test_shunt.assert_called('retrieved')
        except:
            tornado.ioloop.IOLoop.instance().stop()
            raise
