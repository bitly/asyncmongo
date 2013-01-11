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

import glib

class Glib2Stream(object):
    def __init__(self, socket, **kwargs):
        self.__socket = socket
        self.__close_id = None
        self.__read_id = None
        self.__read_queue = []

    def write(self, data):
        self.__socket.send(data)
    
    def read(self, size, callback):
        self.__read_queue.append((size, callback))

        if not self.__read_id:
            self.set_waiting()

    def set_waiting(self):
        if self.__read_id:
            glib.source_remove(self.__read_id)

        self.__read_id = glib.io_add_watch(
            self.__socket,
            glib.IO_IN,
            self.__on_read_callback)

    def set_idle(self):
        if self.__read_id:
            glib.source_remove(self.__read_id)

    def __on_read_callback(self, source, condition):
        if not self.__read_queue:
            self.set_idle()
            return False

        size, callback = self.__read_queue.pop(0)
        data = self.__socket.recv(size)
        callback(data)
        return True

    def set_close_callback(self, callback):
        if self.__close_id:
            glib.source_remove(self.__close_id)

        self.__close_callback = callback
        self.__close_id = glib.io_add_watch(self.__socket,
                                           glib.IO_HUP|glib.IO_ERR,
                                            self.__on_close_callback)

    def __on_close_callback(self, source, cb_condition, *args, **kwargs):
        self.__close_callback()

    def close(self):
        if self.__close_id:
            glib.source_remove(self.__close_id)

        self.__socket.close()

class AsyncBackend(object):
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(AsyncBackend, cls).__new__(
                cls, *args, **kwargs)
        return cls._instance

    def register_stream(self, socket, **kwargs):
        return Glib2Stream(socket, **kwargs)
