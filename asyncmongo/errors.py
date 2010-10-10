
# From http://www.python.org/dev/peps/pep-0249/
# StandardError
#       |__Warning
#       |__Error
#          |__InterfaceError
#          |__DatabaseError
#             |__DataError
#             |__OperationalError
#             |__IntegrityError
#             |__InternalError
#             |__ProgrammingError
#             |__NotSupportedError

class Warning(StandardError):
    pass

class Error(StandardError):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class DataError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    def __init__(self, msg, code=None):
        self.code = code
        self.msg = msg
    
    def __unicode__(self):
        return u'IntegretyError: %s code:%s' % (self.msg, self.code or '')
    
    def __str__(self):
        return str(self.__unicode__())

class InternalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass

class TooManyConnections(Error):
    pass

class InvalidConnection(DatabaseError):
    pass
