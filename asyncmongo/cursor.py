from bson.son import SON
import logging


import helpers
import message

_QUERY_OPTIONS = {
    "tailable_cursor": 2,
    "slave_okay": 4,
    "oplog_replay": 8,
    "no_timeout": 16}

class Cursor(object):
    def __init__(self, connection, dbname, collection):
        assert isinstance(connection, object)
        assert isinstance(dbname, (str, unicode))
        assert isinstance(collection, (str, unicode))
        
        self._connection = connection
        self.__dbname = dbname
        self.__collection = collection
    
    @property
    def full_collection_name(self):
        return '%s.%s' % (self.__dbname, self.__collection)
    
    def find(self, spec=None, fields=None, skip=0, limit=0,
                 timeout=True, snapshot=False, tailable=False, sort=None,
                 max_scan=None, as_class=None,
                 _must_use_master=False, _is_command=False, 
                 callback=None):
        if spec is None:
            spec = {}

        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(skip, int):
            raise TypeError("skip must be an instance of int")
        if not isinstance(limit, int):
            raise TypeError("limit must be an instance of int")
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
        self.__explain = False
        self.__hint = None
        # self.__as_class = as_class
        self.__tz_aware = False #collection.database.connection.tz_aware
        self.__must_use_master = True #_must_use_master
        self.__is_command = False # _is_commandf
        
        self.callback = callback
        self.__id = self._connection.send_message(
            message.query(self.__query_options(),
                          self.full_collection_name,
                          self.__skip, self.__limit,
                          self.__query_spec(), self.__fields), callback=self._handle_response)
    
    def _handle_response(self, result):
        logging.info('%r' % result)
        # {'cursor_id': 0, 'data': [], 'number_returned': 0, 'starting_from': 0}
        self.callback(result['data'])
        self.callback = None
        # TODO: handle get_more; iteration of data
        # if self.__more_data:
        #     self._connection.send_message(
        #         message.get_more(self.full_collection_name,
        #                                self.__limit, self.__id), callback=self._handle_response)
        #     self.__more_data = None
        # else:
        #     self._callback(result)
    
    def __query_options(self):
        """Get the query options string to use for this query.
        """
        options = 0
        if self.__tailable:
            options |= _QUERY_OPTIONS["tailable_cursor"]
        if False: #self.__collection.database.connection.slave_okay:
            options |= _QUERY_OPTIONS["slave_okay"]
        if not self.__timeout:
            options |= _QUERY_OPTIONS["no_timeout"]
        return options
    
    def __query_spec(self):
        """Get the spec to use for a query.
        """
        spec = self.__spec
        if not self.__is_command and "$query" not in self.__spec:
            spec = SON({"$query": self.__spec})
        if self.__ordering:
            spec["$orderby"] = self.__ordering
        if self.__explain:
            spec["$explain"] = True
        if self.__hint:
            spec["$hint"] = self.__hint
        if self.__snapshot:
            spec["$snapshot"] = True
        if self.__max_scan:
            spec["$maxScan"] = self.__max_scan
        return spec
    
    