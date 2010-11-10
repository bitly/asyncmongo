
import tornado.iostream
import socket
import helpers
import struct
import logging

from errors import ProgrammingError, IntegrityError, InterfaceError

# The mongo wire protocol is described at
# http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol

class Connection(object):
    def __init__(self, host, port, slave_ok=False, autoreconnect=True, pool=None):
        assert isinstance(host, (str, unicode))
        assert isinstance(port, int)
        assert isinstance(slave_ok, bool)
        assert isinstance(autoreconnect, bool)
        assert pool
        self.__host = host
        self.__port = port
        self.__slave_ok = slave_ok
        self.__stream = None
        self.__callback = None
        self.__alive = False
        self.__connect()
        self.__autoreconnect = autoreconnect
        self.__pool = pool
        self.usage_count = 0
    
    def __connect(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        s.connect((self.__host, self.__port))
        self.__stream = tornado.iostream.IOStream(s)
        self.__stream.set_close_callback(self._socket_close)
        self.__alive = True
    
    def _socket_close(self):
        self.__alive = False
    
    def close(self):
        self.__alive = False
        self.__stream.close()
    
    def send_message(self, message, callback):
        self.usage_count +=1
        # TODO: handle reconnect
        if self.__callback is not None:
            raise ProgrammingError('connection already in use')
        
        if not self.__alive:
            if self.__autoreconnect:
                self.__connect()
            else:
                raise InterfaceError('connection invalid. autoreconnect=False')
        
        self.__callback=callback
        # __request_id used by get_more()
        (self.__request_id, data) = message
        # logging.info('request id %d writing %r' % (self.__request_id, data))
        try:
            self.__stream.write(data)
            self.__stream.read_bytes(16, callback=self._parse_header)
        except IOError, e:
            self.__alive = False
            raise
        # return self.__request_id 
    
    def _parse_header(self, header):
        # return self.__receive_data_on_socket(length - 16, sock)
        # logging.info('got data %r' % header)
        length = int(struct.unpack("<i", header[:4])[0])
        request_id = struct.unpack("<i", header[8:12])[0]
        assert request_id == self.__request_id, \
            "ids don't match %r %r" % (self.__request_id,
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
        callback = self.__callback
        request_id = self.__request_id
        self.__request_id = None
        self.__callback = None
        self.__pool.cache(self)
        
        try:
            response = helpers._unpack_response(response, request_id) # TODO: pass tz_awar
        except Exception, e:
            logging.error('error %s' % e)
            callback(None, e)
            return
        
        if response and response['data'] and response['data'][0].get('err') and response['data'][0].get('code'):
            # logging.error(response['data'][0]['err'])
            callback(None, IntegrityError(response['data'][0]['err'], code=response['data'][0]['code']))
            return
        # logging.info('response: %s' % response)
        callback(response)


