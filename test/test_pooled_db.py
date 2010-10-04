import test_shunt

from DBUtils import PooledDB
import tornado.ioloop
import logging

import sys
sys.path.append("")

import asyncmongo



output = {}
def test_pooled_db():
    
    pool = PooledDB.PooledDB(asyncmongo, maxconnections=5, host='127.0.0.1', port=27017, dbname='history')
    
    db = pool.dedicated_connection()
    cursor = db.cursor('users')
    db2 = pool.dedicated_connection()
    cursor2 = db2.cursor('users')

    def pool_callback(response):
        assert len(response) == 1
        output['called'] = True
        if "called2" in output:
            tornado.ioloop.IOLoop.instance().stop()

    def pool_callback2(response):
        assert len(response) == 1
        output['called2'] = True
        if "called" in output:
            tornado.ioloop.IOLoop.instance().stop()
    
    cursor.find({}, limit=1, callback=pool_callback)
    cursor2.find({}, limit=1, callback=pool_callback2)
    
    tornado.ioloop.IOLoop.instance().start()
    assert "called" in output
    assert "called2" in output
