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

import logging

from bson.son import SON

import helpers
import message
import functools

_QUERY_OPTIONS = {
    "tailable_cursor": 2,
    "slave_okay": 4,
    "oplog_replay": 8,
    "no_timeout": 16}

class Cursor(object):
    """ Cursor is a class used to call oeprations on a given db/collection using a specific connection pool.
        it will transparently release connections back to the pool after they receive responses
    """
    def __init__(self, dbname, collection, pool):
        assert isinstance(dbname, (str, unicode))
        assert isinstance(collection, (str, unicode))
        assert isinstance(pool, object)
        
        self.__dbname = dbname
        self.__collection = collection
        self.__pool = pool
        self.__slave_okay = False
    
    @property
    def full_collection_name(self):
        return u'%s.%s' % (self.__dbname, self.__collection)
    
    def drop(self, *args, **kwargs):
        raise NotImplemented("patches accepted")

    def save(self, doc, **kwargs):
        assert isinstance(doc, dict)
        self.insert(doc, **kwargs)

    def insert(self, doc_or_docs,
               manipulate=True, safe=True, check_keys=True, callback=None, **kwargs):
        """Insert a document(s) into this collection.
        
        If `manipulate` is set, the document(s) are manipulated using
        any :class:`~pymongo.son_manipulator.SONManipulator` instances
        that have been added to this
        :class:`~pymongo.database.Database`. Returns the ``"_id"`` of
        the inserted document or a list of ``"_id"`` values of the
        inserted documents.  If the document(s) does not already
        contain an ``"_id"`` one will be added.
        
        If `safe` is ``True`` then the insert will be checked for
        errors, raising :class:`~pymongo.errors.OperationFailure` if
        one occurred. Safe inserts wait for a response from the
        database, while normal inserts do not.
        
        Any additional keyword arguments imply ``safe=True``, and
        will be used as options for the resultant `getLastError`
        command. For example, to wait for replication to 3 nodes, pass
        ``w=3``.
        
        :Parameters:
          - `doc_or_docs`: a document or list of documents to be
            inserted
          - `manipulate` (optional): manipulate the documents before
            inserting?
          - `safe` (optional): check that the insert succeeded?
          - `check_keys` (optional): check if keys start with '$' or
            contain '.', raising :class:`~pymongo.errors.InvalidName`
            in either case
          - `**kwargs` (optional): any additional arguments imply
            ``safe=True``, and will be used as options for the
            `getLastError` command
        
        .. mongodoc:: insert
        """
        if not isinstance(safe, bool):
            raise TypeError("safe must be an instance of bool")
        
        docs = doc_or_docs
        # return_one = False
        if isinstance(docs, dict):
            # return_one = True
            docs = [docs]
        
        # if manipulate:
        #     docs = [self.__database._fix_incoming(doc, self) for doc in docs]
        
        self.__limit = None
        if kwargs:
            safe = True
        
        if safe and not callable(callback):
            raise TypeError("callback must be callable")
        if not safe and callback is not None:
            raise TypeError("callback can not be used with safe=False")
        
        if callback:
            callback = functools.partial(self._handle_response, orig_callback=callback)

        connection = self.__pool.connection()
        try:
            connection.send_message(
                message.insert(self.full_collection_name, docs,
                    check_keys, safe, kwargs), callback=callback)
        except:
            connection.close()
            raise
    
    def remove(self, spec_or_id=None, safe=True, callback=None, **kwargs):
        if not isinstance(safe, bool):
            raise TypeError("safe must be an instance of bool")
        
        if spec_or_id is None:
            spec_or_id = {}
        if not isinstance(spec_or_id, dict):
            spec_or_id = {"_id": spec_or_id}
        
        self.__limit = None
        if kwargs:
            safe = True
        
        if safe and not callable(callback):
            raise TypeError("callback must be callable")
        if not safe and callback is not None:
            raise TypeError("callback can not be used with safe=False")
        
        if callback:
            callback = functools.partial(self._handle_response, orig_callback=callback)

        connection = self.__pool.connection()
        try:
            connection.send_message(
                message.delete(self.full_collection_name, spec_or_id, safe, kwargs),
                    callback=callback)
        except:
            connection.close()
            raise

    
    def update(self, spec, document, upsert=False, manipulate=False,
               safe=True, multi=False, callback=None, **kwargs):
        """Update a document(s) in this collection.
        
        Raises :class:`TypeError` if either `spec` or `document` is
        not an instance of ``dict`` or `upsert` is not an instance of
        ``bool``. If `safe` is ``True`` then the update will be
        checked for errors, raising
        :class:`~pymongo.errors.OperationFailure` if one
        occurred. Safe updates require a response from the database,
        while normal updates do not - thus, setting `safe` to ``True``
        will negatively impact performance.
        
        There are many useful `update modifiers`_ which can be used
        when performing updates. For example, here we use the
        ``"$set"`` modifier to modify some fields in a matching
        document:
        
        .. doctest::
          
          >>> db.test.insert({"x": "y", "a": "b"})
          ObjectId('...')
          >>> list(db.test.find())
          [{u'a': u'b', u'x': u'y', u'_id': ObjectId('...')}]
          >>> db.test.update({"x": "y"}, {"$set": {"a": "c"}})
          >>> list(db.test.find())
          [{u'a': u'c', u'x': u'y', u'_id': ObjectId('...')}]
        
        If `safe` is ``True`` returns the response to the *lastError*
        command. Otherwise, returns ``None``.
        
        # Any additional keyword arguments imply ``safe=True``, and will
        # be used as options for the resultant `getLastError`
        # command. For example, to wait for replication to 3 nodes, pass
        # ``w=3``.
        
        :Parameters:
          - `spec`: a ``dict`` or :class:`~bson.son.SON` instance
            specifying elements which must be present for a document
            to be updated
          - `document`: a ``dict`` or :class:`~bson.son.SON`
            instance specifying the document to be used for the update
            or (in the case of an upsert) insert - see docs on MongoDB
            `update modifiers`_
          - `upsert` (optional): perform an upsert if ``True``
          - `manipulate` (optional): manipulate the document before
            updating? If ``True`` all instances of
            :mod:`~pymongo.son_manipulator.SONManipulator` added to
            this :class:`~pymongo.database.Database` will be applied
            to the document before performing the update.
          - `safe` (optional): check that the update succeeded?
          - `multi` (optional): update all documents that match
            `spec`, rather than just the first matching document. The
            default value for `multi` is currently ``False``, but this
            might eventually change to ``True``. It is recommended
            that you specify this argument explicitly for all update
            operations in order to prepare your code for that change.
          - `**kwargs` (optional): any additional arguments imply
            ``safe=True``, and will be used as options for the
            `getLastError` command
        
        .. _update modifiers: http://www.mongodb.org/display/DOCS/Updating
        
        .. mongodoc:: update
        """
        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(document, dict):
            raise TypeError("document must be an instance of dict")
        if not isinstance(upsert, bool):
            raise TypeError("upsert must be an instance of bool")
        if not isinstance(safe, bool):
            raise TypeError("safe must be an instance of bool")
        # TODO: apply SON manipulators
        # if upsert and manipulate:
        #     document = self.__database._fix_incoming(document, self)
        
        if kwargs:
            safe = True
        
        if safe and not callable(callback):
            raise TypeError("callback must be callable")
        if not safe and callback is not None:
            raise TypeError("callback can not be used with safe=False")
        
        if callback:
            callback = functools.partial(self._handle_response, orig_callback=callback)

        self.__limit = None
        connection = self.__pool.connection()
        try:
            connection.send_message(
                message.update(self.full_collection_name, upsert, multi,
                    spec, document, safe, kwargs), callback=callback)
        except:
            connection.close()
            raise

    
    def find_one(self, spec_or_id, **kwargs):
        """Get a single document from the database.
        
        All arguments to :meth:`find` are also valid arguments for
        :meth:`find_one`, although any `limit` argument will be
        ignored. Returns a single document, or ``None`` if no matching
        document is found.
        """
        if spec_or_id is not None and not isinstance(spec_or_id, dict):
            spec_or_id = {"_id": spec_or_id}
        kwargs['limit'] = -1
        self.find(spec_or_id, **kwargs)
    
    def find(self, spec=None, fields=None, skip=0, limit=0,
                 timeout=True, snapshot=False, tailable=False, sort=None,
                 max_scan=None, slave_okay=False,
                 _must_use_master=False, _is_command=False, hint=None, debug=False,
                 comment=None, callback=None):
        """Query the database.
        
        The `spec` argument is a prototype document that all results
        must match. For example:
        
        >>> db.test.find({"hello": "world"}, callback=...)
        
        only matches documents that have a key "hello" with value
        "world".  Matches can have other keys *in addition* to
        "hello". The `fields` argument is used to specify a subset of
        fields that should be included in the result documents. By
        limiting results to a certain subset of fields you can cut
        down on network traffic and decoding time.
        
        Raises :class:`TypeError` if any of the arguments are of
        improper type.
        
        :Parameters:
          - `spec` (optional): a SON object specifying elements which
            must be present for a document to be included in the
            result set
          - `fields` (optional): a list of field names that should be
            returned in the result set ("_id" will always be
            included), or a dict specifying the fields to return
          - `skip` (optional): the number of documents to omit (from
            the start of the result set) when returning the results
          - `limit` (optional): the maximum number of results to
            return
          - `timeout` (optional): if True, any returned cursor will be
            subject to the normal timeout behavior of the mongod
            process. Otherwise, the returned cursor will never timeout
            at the server. Care should be taken to ensure that cursors
            with timeout turned off are properly closed.
          - `snapshot` (optional): if True, snapshot mode will be used
            for this query. Snapshot mode assures no duplicates are
            returned, or objects missed, which were present at both
            the start and end of the query's execution. For details,
            see the `snapshot documentation
            <http://dochub.mongodb.org/core/snapshot>`_.
          - `tailable` (optional): the result of this find call will
            be a tailable cursor - tailable cursors aren't closed when
            the last data is retrieved but are kept open and the
            cursors location marks the final document's position. if
            more data is received iteration of the cursor will
            continue from the last document received. For details, see
            the `tailable cursor documentation
            <http://www.mongodb.org/display/DOCS/Tailable+Cursors>`_.
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for this query. See
            :meth:`~pymongo.cursor.Cursor.sort` for details.
          - `max_scan` (optional): limit the number of documents
            examined when performing the query
          - `slave_okay` (optional): is it okay to connect directly
            to and perform queries on a slave instance
        
        .. mongodoc:: find
        """
        
        if spec is None:
            spec = {}
        
        if limit is None:
            limit = 0

        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(skip, int):
            raise TypeError("skip must be an instance of int")
        if not isinstance(limit, int):
            raise TypeError("limit must be an instance of int or None")
        if not isinstance(timeout, bool):
            raise TypeError("timeout must be an instance of bool")
        if not isinstance(snapshot, bool):
            raise TypeError("snapshot must be an instance of bool")
        if not isinstance(tailable, bool):
            raise TypeError("tailable must be an instance of bool")
        if not callable(callback):
            raise TypeError("callback must be callable")
        
        if fields is not None:
            if not fields:
                fields = {"_id": 1}
            if not isinstance(fields, dict):
                fields = helpers._fields_list_to_dict(fields)
        
        self.__spec = spec
        self.__fields = fields
        self.__skip = skip
        self.__limit = limit
        self.__batch_size = 0
        
        self.__timeout = timeout
        self.__tailable = tailable
        self.__snapshot = snapshot
        self.__ordering = sort and helpers._index_document(sort) or None
        self.__max_scan = max_scan
        self.__slave_okay = slave_okay
        self.__explain = False
        self.__hint = hint
        self.__comment = comment
        self.__debug = debug
        # self.__as_class = as_class
        self.__tz_aware = False #collection.database.connection.tz_aware
        self.__must_use_master = _must_use_master
        self.__is_command = _is_command
        
        connection = self.__pool.connection()
        try:
            if self.__debug:
                logging.debug('QUERY_SPEC: %r' % self.__query_spec())

            connection.send_message(
                message.query(self.__query_options(),
                              self.full_collection_name,
                              self.__skip, 
                              self.__limit,
                              self.__query_spec(),
                              self.__fields), 
                callback=functools.partial(self._handle_response, orig_callback=callback))
        except Exception, e:
            logging.debug('Error sending query %s' % e)
            connection.close()
            raise
    
    def _handle_response(self, result, error=None, orig_callback=None):
        if result and result.get('cursor_id'):
            connection = self.__pool.connection()
            try:
                connection.send_message(
                    message.kill_cursors([result['cursor_id']]),
                    callback=None)
            except Exception, e:
                logging.debug('Error killing cursor %s: %s' % (result['cursor_id'], e))
                connection.close()
                raise
        
        if error:
            logging.debug('%s %s' % (self.full_collection_name , error))
            orig_callback(None, error=error)
        else:
            if self.__limit == -1 and len(result['data']) == 1:
                # handle the find_one() call
                orig_callback(result['data'][0], error=None)
            else:
                orig_callback(result['data'], error=None)

    
    def __query_options(self):
        """Get the query options string to use for this query."""
        options = 0
        if self.__tailable:
            options |= _QUERY_OPTIONS["tailable_cursor"]
        if self.__slave_okay or self.__pool._slave_okay:
            options |= _QUERY_OPTIONS["slave_okay"]
        if not self.__timeout:
            options |= _QUERY_OPTIONS["no_timeout"]
        return options
    
    def __query_spec(self):
        """Get the spec to use for a query."""
        spec = self.__spec
        if not self.__is_command and "$query" not in self.__spec:
            spec = SON({"$query": self.__spec})
        if self.__ordering:
            spec["$orderby"] = self.__ordering
        if self.__explain:
            spec["$explain"] = True
        if self.__hint:
            spec["$hint"] = self.__hint
        if self.__comment:
            spec["$comment"] = self.__comment
        if self.__snapshot:
            spec["$snapshot"] = True
        if self.__max_scan:
            spec["$maxScan"] = self.__max_scan
        return spec
    
    
