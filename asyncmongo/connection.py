#!/bin/env python
# 
# Copyright 2010 bit.ly
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import sys
import socket
import struct
import logging
from types import NoneType
import functools

from errors import ProgrammingError, IntegrityError, InterfaceError
import helpers
import asyncjobs


class Connection(object):
    """
    :Parameters:
      - `host`: hostname or ip of mongo host (not allowed when replica sets are used)
      - `port`: port to connect to (not allowed when replica sets are used)
      - `dbuser`: db user to connect with
      - `dbpass`: db password
      - `autoreconnect` (optional): auto reconnect on interface errors
      - `rs`: replica set name (required when replica sets are used)
      - `seed`: seed list to connect to a replica set (required when replica sets are used)
      - `secondary_only`: (optional, only useful for replica set connections)
         if true, connect to a secondary member only
      - `**kwargs`: passed to `backends.AsyncBackend.register_stream`

    """
    def __init__(self,
                 host=None,
                 port=None,
                 dbuser=None,
                 dbpass=None,
                 autoreconnect=True,
                 pool=None,
                 backend="tornado",
                 rs=None,
                 seed=None,
                 secondary_only=False,
                 **kwargs):
        assert isinstance(autoreconnect, bool)
        assert isinstance(dbuser, (str, unicode, NoneType))
        assert isinstance(dbpass, (str, unicode, NoneType))
        assert isinstance(rs, (str, NoneType))
        assert pool
        assert isinstance(secondary_only, bool)
        
        if rs:
            assert host is None
            assert port is None
            assert isinstance(seed, (set, list))
        else:
            assert isinstance(host, (str, unicode))
            assert isinstance(port, int)
            assert seed is None
        
        self._host = host
        self._port = port
        self.__rs = rs
        self.__seed = seed
        self.__secondary_only = secondary_only
        self.__dbuser = dbuser
        self.__dbpass = dbpass
        self.__stream = None
        self.__callback = None
        self.__alive = False
        self.__autoreconnect = autoreconnect
        self.__pool = pool
        self.__kwargs = kwargs
        self.__backend = self.__load_backend(backend)
        self.__job_queue = []
        self.usage_count = 0

        self.__connect(self.connection_error)

    def connection_error(self, error):
        raise error

    def __load_backend(self, name):
        __import__('asyncmongo.backends.%s_backend' % name)
        mod = sys.modules['asyncmongo.backends.%s_backend' % name]
        return mod.AsyncBackend()
    
    def __connect(self, err_callback):
        # The callback is only called in case of exception by async jobs
        if self.__dbuser and self.__dbpass:
            self._put_job(asyncjobs.AuthorizeJob(self, self.__dbuser, self.__dbpass, self.__pool, err_callback))

        if self.__rs:
            self._put_job(asyncjobs.ConnectRSJob(self, self.__seed, self.__rs, self.__secondary_only, err_callback))
            # Mark the connection as alive, even though it's not alive yet to prevent double-connecting
            self.__alive = True
        else:
            self._socket_connect()

    def _socket_connect(self):
        """create a socket, connect, register a stream with the async backend"""
        self.usage_count = 0
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            s.connect((self._host, self._port))
            self.__stream = self.__backend.register_stream(s, **self.__kwargs)
            self.__stream.set_close_callback(self._socket_close)
            self.__alive = True
        except socket.error, error:
            raise InterfaceError(error)
    
    def _socket_close(self):
        """cleanup after the socket is closed by the other end"""
        callback = self.__callback
        self.__callback = None
        try:
            if callback:
                callback(None, InterfaceError('connection closed'))
        finally:
            # Flush the job queue, don't call the callbacks associated with the remaining jobs
            # since they have already been called as error callback on connection closing
            self.__job_queue = []
            self.__alive = False
            self.__pool.cache(self)
    
    def _close(self):
        """close the socket and cleanup"""
        callback = self.__callback
        self.__callback = None
        try:
            if callback:
                callback(None, InterfaceError('connection closed'))
        finally:
            # Flush the job queue, don't call the callbacks associated with the remaining jobs
            # since they have already been called as error callback on connection closing
            self.__job_queue = []
            self.__alive = False
            self.__stream.close()

    def close(self):
        """close this connection; re-cache this connection object"""
        try:
            self._close()
        finally:
            self.__pool.cache(self)

    def send_message(self, message, callback):
        """ send a message over the wire; callback=None indicates a safe=False call where we write and forget about it"""
        
        if self.__callback is not None:
            raise ProgrammingError('connection already in use')

        if callback:
            err_callback = functools.partial(callback, None)
        else:
            err_callback = None

        # Go and update err_callback for async jobs in queue if any
        for job in self.__job_queue:
            # this is a dirty hack and I hate it, but there is no way of setting the correct
            # err_callback during the connection time
            if isinstance(job, asyncjobs.AsyncJob):
                job.update_err_callback(err_callback)

        if not self.__alive:
            if self.__autoreconnect:
                self.__connect(err_callback)
            else:
                raise InterfaceError('connection invalid. autoreconnect=False')
        
        # Put the current message on the bottom of the queue
        self._put_job(asyncjobs.AsyncMessage(self, message, callback), 0)
        self._next_job()
        
    def _put_job(self, job, pos=None):
        if pos is None:
            pos = len(self.__job_queue)
        self.__job_queue.insert(pos, job)

    def _next_job(self):
        """execute the next job from the top of the queue"""
        if self.__job_queue:
            # Produce message from the top of the queue
            job = self.__job_queue.pop()
            # logging.debug("queue = %s, popped %r", self.__job_queue, job)
            job.process()
    
    def _send_message(self, message, callback):
        # logging.debug("_send_message, msg = %r: queue = %r, self.__callback = %r, callback = %r", 
        #               message, self.__job_queue, self.__callback, callback)

        self.__callback = callback
        self.usage_count +=1
        # __request_id used by get_more()
        (self.__request_id, data) = message
        try:
            self.__stream.write(data)
            if self.__callback:
                self.__stream.read(16, callback=self._parse_header)
            else:
                self.__request_id = None
                self.__pool.cache(self)
        
        except IOError:
            self.__alive = False
            raise
        # return self.__request_id 
    
    def _parse_header(self, header):
        # return self.__receive_data_on_socket(length - 16, sock)
        length = int(struct.unpack("<i", header[:4])[0])
        request_id = struct.unpack("<i", header[8:12])[0]
        assert request_id == self.__request_id, \
            "ids don't match %r %r" % (self.__request_id,
                                       request_id)
        operation = 1 # who knows why
        assert operation == struct.unpack("<i", header[12:])[0]
        try:
            self.__stream.read(length - 16, callback=self._parse_response)
        except IOError:
            self.__alive = False
            raise
    
    def _parse_response(self, response):
        callback = self.__callback
        request_id = self.__request_id
        self.__request_id = None
        self.__callback = None
        if not self.__job_queue:
            # skip adding to the cache because there is something else
            # that needs to be called on this connection for this request
            # (ie: we authenticated, but still have to send the real req)
            self.__pool.cache(self)

        try:
            response = helpers._unpack_response(response, request_id) # TODO: pass tz_awar
        except Exception, e:
            logging.debug('error %s' % e)
            callback(None, e)
            return
        
        if response and response['data'] and response['data'][0].get('err') and response['data'][0].get('code'):
            callback(response, IntegrityError(response['data'][0]['err'], code=response['data'][0]['code']))
            return
        callback(response)
