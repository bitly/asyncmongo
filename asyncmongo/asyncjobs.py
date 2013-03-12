#!/bin/env python
#
# Copyright 2013 bit.ly
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

"""Tools for creating `messages
<http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol>`_ to be sent to
MongoDB.

.. note:: This module is for internal use and is generally not needed by
   application developers.
"""

import logging
from bson import SON

import message
import helpers
from errors import AuthenticationError, RSConnectionError, InterfaceError

class AsyncMessage(object):
    def __init__(self, connection, message, callback):
        super(AsyncMessage, self).__init__()
        self.connection = connection
        self.message = message
        self.callback = callback

    def process(self, *args, **kwargs):
        try:
            self.connection._send_message(self.message, self.callback)
        except Exception, e:
            if self.callback is None:
                logging.error("Error occurred in safe update mode: %s", e)
            else:
                self.callback(None, e)

class AuthorizeJob(object):
    def __init__(self, connection, dbuser, dbpass, pool):
        super(AuthorizeJob, self).__init__()
        self.connection = connection
        self._state = "start"
        self.dbuser = dbuser
        self.dbpass = dbpass
        self.pool = pool

    def __repr__(self):
        return "AuthorizeJob at 0x%X, state = %r" % (id(self), self._state)

    def process(self, response=None, error=None):
        if error:
            logging.debug(error)
            logging.debug(response)
            raise AuthenticationError(error)

        if self._state == "start":
            self._state = "nonce"
            logging.debug("Sending nonce")
            msg = message.query(
                0,
                "%s.$cmd" % self.pool._dbname,
                0,
                1,
                SON({'getnonce': 1}),
                SON({})
            )
            self.connection._send_message(msg, self.process)
        elif self._state == "nonce":
            # this is the nonce response
            self._state = "finish"
            nonce = response['data'][0]['nonce']
            logging.debug("Nonce received: %r", nonce)
            key = helpers._auth_key(nonce, self.dbuser, self.dbpass)

            msg = message.query(
                0,
                "%s.$cmd" % self.pool._dbname,
                0,
                1,
                SON([('authenticate', 1),
                     ('user', self.dbuser),
                     ('nonce', nonce),
                     ('key', key)]),
                SON({})
            )
            self.connection._send_message(msg, self.process)
        elif self._state == "finish":
            self._state = "done"
            assert response['number_returned'] == 1
            response = response['data'][0]
            if response['ok'] != 1:
                logging.debug('Failed authentication %s' % response['errmsg'])
                raise AuthenticationError(response['errmsg'])
            self.connection._next_job()
        else:
            raise ValueError("Unexpected state: %s" % self._state)

class ConnectRSJob(object):
    def __init__(self, connection, seed, rs):
        self.connection = connection
        self.known_hosts = set(seed)
        self.rs = rs
        self._tried_hosts = set()
        self._state = "seed"
        self._primary = None

    def __repr__(self):
        return "ConnectRSJob at 0x%X, state = %s" % (id(self), self._state)

    def process(self, response=None, error=None):
        if error:
            logging.debug("Problem connecting: %s", error)

            if self._state == "ismaster":
                self._state = "seed"

        if self._state == "seed":
            fresh = self.known_hosts ^ self._tried_hosts
            logging.debug("Working through the rest of the host list: %r", fresh)

            while fresh:
                if self._primary and self._primary not in self._tried_hosts:
                    # Try primary first
                    h = self._primary
                else:
                    h = fresh.pop()

                self._tried_hosts.add(h)

                logging.debug("Connecting to %s:%s", *h)
                self.connection._host, self.connection._port = h
                try:
                    self.connection._socket_connect()
                    logging.debug("Connected to %s", h)
                except InterfaceError, e:
                    logging.error("Failed to connect to the host: %s", e)
                else:
                    break

            else:
                raise RSConnectionError("No more hosts to try, tried: %s" % self.known_hosts)

            self._state = "ismaster"
            msg = message.query(
                options=0,
                collection_name="admin.$cmd",
                num_to_skip=0,
                num_to_return=-1,
                query=SON([("ismaster", 1)])
            )
            self.connection._send_message(msg, self.process)

        elif self._state == "ismaster":
            logging.debug("ismaster response: %r", response)

            if len(response["data"]) == 1:
                res = response["data"][0]
            else:
                raise RSConnectionError("Invalid response data: %r" % response["data"])

            rs_name = res.get("setName")
            if rs_name:
                if rs_name != self.rs:
                    raise RSConnectionError("Wrong replica set: %s, expected: %s" %
                                            (rs_name, self.rs))
            hosts = res.get("hosts")
            if hosts:
                self.known_hosts.update(helpers._parse_host(h) for h in hosts)

            ismaster = res.get("ismaster")
            if ismaster:
                logging.info("Connected to master")
                self._state = "done"
                self.connection._next_job()
            else:
                primary = res.get("primary")
                if primary:
                    self._primary = helpers._parse_host(primary)

                self._state = "seed"
                self.process()
