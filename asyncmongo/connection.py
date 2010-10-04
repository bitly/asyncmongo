
import tornado.iostream
import socket
import cursor
import helpers
import struct
import logging

# The mongo wire protocol is described at
# http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol

class Connection(object):
    def __init__(self, host, port, dbname, slave_ok=False):
        assert isinstance(host, (str, unicode))
        assert isinstance(port, int)
        assert isinstance(slave_ok, bool)
        self.__host = host
        self.__port = port
        self.__dbname = dbname
        self.__slave_ok = slave_ok
        self.__stream = None
        self.__connect()
    
    def __connect(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        s.connect((self.__host, self.__port))
        self.__stream = tornado.iostream.IOStream(s)
        self.__stream.set_close_callback(self._socket_close)
        
    def _socket_close(self):
        pass
        
    def commit(self):
        pass

    def rollback(self):
        pass
    
    def cursor(self, name):
        return cursor.Cursor(self, self.__dbname, name)

    def close(self):
        self.__stream.close()
    
    def __getattr__(self, name):
        """Get a collection by name.

        :Parameters:
          - `name`: the name of the collection
        """
        return cursor.Cursor(self, self.__dbname, name)

    def __getitem__(self, name):
        """Get a collection by name.
        :Parameters:
          - `name`: the name of the collection to get
        """
        return self.__getattr__(name)
        
    def send_message(self, message, callback):
        # TODO: handle reconnect
        
        self._callback=callback
        (self._request_id, data) = message
        logging.info('request id %d' % self._request_id)
        self.__stream.write(data)
        self.__stream.read_bytes(16, callback=self._parse_header)
        return self._request_id # used by get_more()
    
    def _parse_header(self, header):
        # return self.__receive_data_on_socket(length - 16, sock)
        logging.info('got data %r' % header)
        length = int(struct.unpack("<i", header[:4])[0])
        request_id = struct.unpack("<i", header[8:12])[0]
        assert request_id == self._request_id, \
            "ids don't match %r %r" % (self._request_id,
                                       request_id)
        operation = 1 # who knows why
        assert operation == struct.unpack("<i", header[12:])[0]
        logging.info('%s' % length)
        # logging.info('waiting for another %d bytes' % length - 16)
        self.__stream.read_bytes(length - 16, callback=self._parse_response)

    def _parse_response(self, response):
        logging.info('got data %r' % response)
        response = helpers._unpack_response(response, self._request_id) # TODO: pass tz_awar
        logging.info('response: %s' % response)
        self._callback(response)
        self._callback = None
        self._request_id = None


