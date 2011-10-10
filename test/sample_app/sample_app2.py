#!/usr/bin/env python

#   mkdir /tmp/asyncmongo_sample_app2
#   mongod --port 27017 --oplogSize 10 --dbpath /tmp/asyncmongo_sample_app2

#   $mongo
#   >>>use test;
#   db.addUser("testuser", "testpass");

#   ab  -n 1000 -c 16 http://127.0.0.1:8888/ 

import sys
import logging
import os
app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if app_dir not in sys.path:
    logging.debug('adding %r to sys.path' % app_dir)
    sys.path.insert(0, app_dir)

import asyncmongo
# make sure we get the local asyncmongo
assert asyncmongo.__file__.startswith(app_dir)

import tornado.ioloop
import tornado.web
import tornado.options

class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        db.users.find_one({"user_id" : 1}, callback=self._on_response)

    def _on_response(self, response, error):
        assert not error
        self.write(str(response))
        self.finish()
        

if __name__ == "__main__":
    tornado.options.parse_command_line()
    application = tornado.web.Application([
            (r"/?", MainHandler)
            ])
    application.listen(8888)
    db = asyncmongo.Client(pool_id="test",
                           host='127.0.0.1',
                           port=27017,
                           mincached=5,
                           maxcached=15,
                           maxconnections=30,
                           dbname='test', 
                           dbuser='testuser',
                           dbpass='testpass')
    tornado.ioloop.IOLoop.instance().start()
