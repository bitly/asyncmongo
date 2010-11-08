
import tornado.iostream
import socket
import cursor
import helpers
import struct
import logging

from errors import DataError, ProgrammingError, IntegrityError, InterfaceError

# The mongo wire protocol is described at
# http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol

class Connection(object):
    def __init__(self, host, port, dbname, slave_ok=False, autoreconnect=True):
        assert isinstance(host, (str, unicode))
        assert isinstance(port, int)
        assert isinstance(slave_ok, bool)
        assert isinstance(autoreconnect, bool)
        self.__host = host
        self.__port = port
        self.__dbname = dbname
        self.__slave_ok = slave_ok
        self.__stream = None
        self.__callback = None
        self.__alive = False
        self.__connect()
        self.__autoreconnect = autoreconnect
        self.__pool = None
    
    def __connect(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        s.connect((self.__host, self.__port))
        self.__stream = tornado.iostream.IOStream(s)
        self.__stream.set_close_callback(self._socket_close)
        self.__alive = True
        
    def _socket_close(self):
        self.__alive = False
        
    def commit(self):
        pass

    def rollback(self):
        pass
    
    def cursor(self, name, pool=None):
        """Get a cursor to a collection by name.

        raises `DataError` on names with unallowable characters.

        :Parameters:
          - `name`: the name of the collection
        """
        if not name or ".." in name:
            raise DataError("collection names cannot be empty")
        if "$" in name and not (name.startswith("oplog.$main") or
                                name.startswith("$cmd")):
            raise DataError("collection names must not "
                              "contain '$': %r" % name)
        if name.startswith(".") or name.endswith("."):
            raise DataError("collecion names must not start "
                            "or end with '.': %r" % name)
        if "\x00" in name:
            raise DataError("collection names must not contain the "
                              "null character")
        return cursor.Cursor(self, self.__dbname, name, pool)

    def threadsafety(self):
        return 2

    def close(self):
        self.__alive = False
        self.__stream.close()
    
    def __getattr__(self, name):
        """Get a collection by name.

        :Parameters:
          - `name`: the name of the collection
        """
        return self.cursor(name, self)

    def __getitem__(self, name):
        """Get a collection by name.
        :Parameters:
          - `name`: the name of the collection to get
        """
        return self.cursor(name, self)
        
    def send_message(self, message, callback):
        # TODO: handle reconnect
        if self.__callback is not None:
            raise ProgrammingError('connection already in use')

        if not self.__alive:
            if self.__autoreconnect:
                self.__connect()
            else:
                raise InterfaceError('connection invalid. autoreconnect=False')
        
        self.__callback=callback
        (self._request_id, data) = message
        # logging.info('request id %d writing %r' % (self._request_id, data))
        try:
            self.__stream.write(data)
            self.__stream.read_bytes(16, callback=self._parse_header)
        except IOError, e:
            self.__alive = False
            raise
        return self._request_id # used by get_more()
    
    def _parse_header(self, header):
        # return self.__receive_data_on_socket(length - 16, sock)
        # logging.info('got data %r' % header)
        length = int(struct.unpack("<i", header[:4])[0])
        request_id = struct.unpack("<i", header[8:12])[0]
        assert request_id == self._request_id, \
            "ids don't match %r %r" % (self._request_id,
                                       request_id)
        operation = 1 # who knows why
        assert operation == struct.unpack("<i", header[12:])[0]
        # logging.info('%s' % length)
        # logging.info('waiting for another %d bytes' % length - 16)
        try:
            self.__stream.read_bytes(length - 16, callback=self._parse_response)
        except IOError, e:
            self.__alive = False
            raise

    def _parse_response(self, response):
        # logging.info('got data %r' % response)
        try:
            response = helpers._unpack_response(response, self._request_id) # TODO: pass tz_awar
        except Exception, e:
            logging.error('error %s' % e)
            self.__callback(None, e)
            self.__callback = None
            self.__request_id = None
            return

        if response and response['data'] and response['data'][0].get('err') and response['data'][0].get('code'):
            # logging.error(response['data'][0]['err'])
            self.__callback(None, IntegrityError(response['data'][0]['err'], code=response['data'][0]['code']))
            self.__callback = None
            self.__request_id = None
            return
        # logging.info('response: %s' % response)
        self.__callback(response)
        self.__callback = None
        self.__request_id = None


