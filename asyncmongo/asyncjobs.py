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
import random
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


class AsyncJob(object):
    def __init__(self, connection, state, err_callback):
        super(AsyncJob, self).__init__()
        self.connection = connection
        self._err_callback = err_callback
        self._state = state

    def _error(self, e):
        self.connection.close()
        if self._err_callback:
            self._err_callback(e)

    def update_err_callback(self, err_callback):
        self._err_callback = err_callback

    def __repr__(self):
        return "%s at 0x%X, state = %r" % (self.__class__.__name__, id(self), self._state)


class AuthorizeJob(AsyncJob):
    def __init__(self, connection, dbuser, dbpass, pool, err_callback):
        super(AuthorizeJob, self).__init__(connection, "start", err_callback)
        self.dbuser = dbuser
        self.dbpass = dbpass
        self.pool = pool

    def process(self, response=None, error=None):
        if error:
            logging.debug("Error during authentication: %r", error)
            self._error(AuthenticationError(error))
            return

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
            try:
                nonce = response['data'][0]['nonce']
                logging.debug("Nonce received: %r", nonce)
                key = helpers._auth_key(nonce, self.dbuser, self.dbpass)
            except Exception, e:
                self._error(AuthenticationError(e))
                return

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
            try:
                assert response['number_returned'] == 1
                response = response['data'][0]
            except Exception, e:
                self._error(AuthenticationError(e))
                return

            if response.get("ok") != 1:
                logging.debug("Failed authentication %s", response.get("errmsg"))
                self._error(AuthenticationError(response.get("errmsg")))
                return
            self.connection._next_job()
        else:
            self._error(ValueError("Unexpected state: %s" % self._state))


class ConnectRSJob(AsyncJob):
    def __init__(self, connection, seed, rs, secondary_only, err_callback):
        super(ConnectRSJob, self).__init__(connection, "seed", err_callback)
        self.known_hosts = set(seed)
        self.rs = rs
        self._blacklisted = set()
        self._primary = None
        self._sec_only = secondary_only

    def process(self, response=None, error=None):
        if error:
            logging.debug("Problem connecting: %s", error)

            if self._state == "ismaster":
                self._state = "seed"

        if self._state == "seed":
            if self._sec_only and self._primary:
                # Add primary host to blacklisted to avoid connecting to it
                self._blacklisted.add(self._primary)

            fresh = self.known_hosts ^ self._blacklisted
            logging.debug("Working through the rest of the host list: %r", fresh)

            while fresh:
                if self._primary and self._primary not in self._blacklisted:
                    # Try primary first
                    h = self._primary
                else:
                    h = random.choice(list(fresh))

                if h in fresh:
                    fresh.remove(h)

                # Add tried host to blacklisted
                self._blacklisted.add(h)

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
                self._error(RSConnectionError("No more hosts to try, tried: %s" % self.known_hosts))
                return

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

            try:
                assert len(response["data"]) == 1
                res = response["data"][0]
            except Exception, e:
                self._error(RSConnectionError("Invalid response data: %r" % response.get("data")))
                return

            rs_name = res.get("setName")
            if rs_name and rs_name != self.rs:
                self._error(RSConnectionError("Wrong replica set: %s, expected: %s" % (rs_name, self.rs)))
                return

            hosts = res.get("hosts")
            if hosts:
                self.known_hosts.update(helpers._parse_host(h) for h in hosts)

            ismaster = res.get("ismaster")
            hidden = res.get("hidden")
            try:
                if ismaster and not self._sec_only:  # master and required to connect to primary
                    assert not hidden, "Primary cannot be hidden"
                    logging.debug("Connected to master (%s)", res.get("me", "unknown"))
                    self._state = "done"
                    self.connection._next_job()
                elif not ismaster and self._sec_only and not hidden:  # not master and required to connect to secondary
                    assert res.get("secondary"), "Secondary must self-report as secondary"
                    logging.debug("Connected to secondary (%s)", res.get("me", "unknown"))
                    self._state = "done"
                    self.connection._next_job()
                else:  # either not master and primary connection required or master and secondary required
                    primary = res.get("primary")
                    if primary:
                        self._primary = helpers._parse_host(primary)
                    self._state = "seed"
                    self.process()
            except Exception, e:
                self._error(RSConnectionError(e))
                return

