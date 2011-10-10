#!/usr/bin/env python

import os
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.options
import logging
import simplejson as json
import asyncmongo
import pymongo.json_util
import base64
import settings


class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        if not hasattr(self, "_db"):
            self._db = asyncmongo.Client(pool_id='test_pool', **settings.get('mongo_database'))
        return self._db
    
    def api_response(self, data):
        """return an api response in the proper output format with status_code == 200"""
        self.set_header("Content-Type", "application/javascript; charset=UTF-8")
        data = json.dumps(data, default=pymongo.json_util.default)
        self.finish(data)


class Put(BaseHandler):
    @tornado.web.asynchronous
    def get(self):
        rand = base64.b64encode(os.urandom(32))
        try:
            self.db.test.insert({ 'blah': rand }, callback=self.async_callback(self.finish_save))
        except Exception, e:
            logging.error(e)
            return self.api_response({'status':'ERROR', 'status_string': '%s' % e})
    
    def finish_save(self, response, error):
        if error or response[0].get('ok') != 1:
            logging.error(error)
            raise tornado.web.HTTPError(500, 'QUERY_ERROR')
        
        self.api_response({'status':'OK', 'status_string': 'record(%s) saved' % response})


class Application(tornado.web.Application):
    def __init__(self):
        debug = tornado.options.options.environment == "dev"
        app_settings = { 'debug':debug }
        
        handlers = [
            (r"/put", Put)
        ]
        
        tornado.web.Application.__init__(self, handlers, **app_settings)


if __name__ == "__main__":
    tornado.options.define("port", type=int, default=5150, help="Listen port")
    tornado.options.parse_command_line()
    
    logging.info("starting webserver on 0.0.0.0:%d" % tornado.options.options.port)
    http_server = tornado.httpserver.HTTPServer(request_callback=Application())
    http_server.listen(tornado.options.options.port)
    tornado.ioloop.IOLoop.instance().start()
