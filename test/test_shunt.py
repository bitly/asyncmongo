import logging
import sys
import os
import unittest
import subprocess
import signal
import time

import tornado.ioloop
import pymongo

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
   format='%(asctime)s %(process)d %(filename)s %(lineno)d %(levelname)s #| %(message)s',
   datefmt='%H:%M:%S')

# add the path to the local asyncmongo
# there is probably a better way to do this that doesn't require magic
app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if app_dir not in sys.path:
    logging.debug('adding %r to sys.path' % app_dir)
    sys.path.insert(0, app_dir)

import asyncmongo
import asyncmongo.pool
# make sure we get the local asyncmongo
assert asyncmongo.__file__.startswith(app_dir)

class PuritanicalIOLoop(tornado.ioloop.IOLoop):
    """
    A loop that quits when it encounters an Exception -- makes errors in
    callbacks easier to debug and prevents them from hanging the unittest
    suite.
    """
    def handle_callback_exception(self, callback):
        exc_type, exc_value, tb = sys.exc_info()
        raise exc_value

class MongoTest(unittest.TestCase):
    """
    Starts and stops a mongod
    """
    mongod_options = [('--port', str(27018))]
    def setUp(self):
        """setup method that starts up mongod instances using `self.mongo_options`"""
        # So any function that calls IOLoop.instance() gets the
        # PuritanicalIOLoop instead of a default loop.
        if not tornado.ioloop.IOLoop.initialized():
            self.loop = PuritanicalIOLoop()
            tornado.ioloop.IOLoop._instance = self.loop
        else:
            self.loop = tornado.ioloop.IOLoop.instance()
            self.assert_(
                isinstance(self.loop, PuritanicalIOLoop),
                "Couldn't install IOLoop"
            )
            
        self.temp_dirs = []
        self.mongods = []
        for options in self.mongod_options:
            dirname = os.tempnam()
            os.makedirs(dirname)
            self.temp_dirs.append(dirname)
            
            options = ['mongod', '--oplogSize', '2', '--dbpath', dirname,
                       '--smallfiles', '-v', '--nojournal', '--bind_ip', '0.0.0.0'] + list(options)
            logging.debug(options)
            pipe = subprocess.Popen(options)
            self.mongods.append(pipe)
            logging.debug('started mongod %s' % pipe.pid)
        sleep_time = 1 + (len(self.mongods) * 2)
        logging.info('waiting for mongod to start (sleeping %d seconds)' % sleep_time)
        time.sleep(sleep_time)

    def tearDown(self):
        """teardown method that cleans up child mongod instances, and removes their temporary data files"""
        logging.debug('teardown')
        asyncmongo.pool.ConnectionPools.close_idle_connections()
        for mongod in self.mongods:
            logging.debug('killing mongod %s' % mongod.pid)
            os.kill(mongod.pid, signal.SIGKILL)
            mongod.wait()
        for dirname in self.temp_dirs:
            logging.debug('cleaning up %s' % dirname)
            pipe = subprocess.Popen(['rm', '-rf', dirname])
            pipe.wait()


class SynchronousMongoTest(unittest.TestCase):
    """
    Convenience class: a test case that can make synchronous calls to the
    official pymongo to ease setup code, via the pymongo_conn property.
    """
    mongod_options = [('--port', str(27018))]
    @property
    def pymongo_conn(self):
        if not hasattr(self, '_pymongo_conn'):
            self._pymongo_conn = pymongo.Connection(port=int(self.mongod_options[0][1]))
        return self._pymongo_conn

    def get_open_cursors(self):
        output = self.pymongo_conn.admin.command('serverStatus')
        return output.get('cursors', {}).get('totalOpen')

results = {}

def setup():
    global results
    results = {}

def register_called(key, data=None):
    assert key not in results
    results[key] = data

def assert_called(key, data=None):
    assert key in results
    assert results[key] == data

def is_called(key):
    return key in results
