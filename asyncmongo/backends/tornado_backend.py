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

import tornado.iostream

class TornadoStream(object):
    def __init__(self, socket, **kwargs):
        """
        :Parameters:
          - `socket`: TCP socket
          - `**kwargs`: passed to `tornado.iostream.IOStream`
            - `io_loop` (optional): Tornado IOLoop instance.
            - `max_buffer_size` (optional):
            - `read_chunk_size` (optional):
        """
        self.__stream = tornado.iostream.IOStream(socket, **kwargs)

    def write(self, data):
        self.__stream.write(data)
    
    def read(self, size, callback):
        self.__stream.read_bytes(size, callback=callback)

    def set_close_callback(self, callback):
        self.__stream.set_close_callback(callback)

    def close(self):
        self.__stream._close_callback = None
        self.__stream.close()

class AsyncBackend(object):
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(AsyncBackend, cls).__new__(
                cls, *args, **kwargs)
        return cls._instance

    def register_stream(self, socket, **kwargs):
        """
        :Parameters:
          - `socket`: TCP socket
          - `**kwargs`: passed to `tornado.iostream.IOStream`
            - `io_loop` (optional): Tornado IOLoop instance.
            - `max_buffer_size` (optional):
            - `read_chunk_size` (optional):
        """
        return TornadoStream(socket, **kwargs)
