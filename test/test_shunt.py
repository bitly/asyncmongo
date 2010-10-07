import logging
import sys
import os

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
   format='%(asctime)s %(process)d %(filename)s %(lineno)d %(levelname)s #| %(message)s',
   datefmt='%H:%M:%S')

# add the path to the local asyncmongo
# there is probably a better way to do this that doesn't require magic
app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if app_dir not in sys.path:
    logging.info('adding %r to sys.path' % app_dir)
    sys.path.insert(0, app_dir)

import asyncmongo
# make sure we get the local asyncmongo
assert asyncmongo.__file__.startswith(app_dir)


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
