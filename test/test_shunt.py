import logging
import sys
import os
import unittest
import subprocess
import signal
import time

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
# make sure we get the local asyncmongo
assert asyncmongo.__file__.startswith(app_dir)


class MongoTest(unittest.TestCase):
    mongod_options = [('--port', str(27017))]
    def setUp(self):
        """setup method that starts up mongod instances using `self.mongo_options`"""
        self.temp_dirs = []
        self.mongods = []
        for options in self.mongod_options:
            dirname = os.tempnam()
            os.makedirs(dirname)
            self.temp_dirs.append(dirname)
            
            options = ['mongod', '--bind_ip', '127.0.0.1', '--oplogSize', '10', '--dbpath', dirname] + list(options)
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
        for mongod in self.mongods:
            logging.debug('killing mongod %s' % mongod.pid)
            os.kill(mongod.pid, signal.SIGKILL)
            mongod.wait()
        for dirname in self.temp_dirs:
            logging.debug('cleaning up %s' % dirname)
            pipe = subprocess.Popen(['rm', '-rf', dirname])
            pipe.wait()

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
