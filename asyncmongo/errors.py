
# StandardError
#       |__Error
#          |__InterfaceError
#          |__DatabaseError
#             |__DataError
#             |__IntegrityError
#             |__ProgrammingError
#             |__NotSupportedError

class Error(StandardError):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class DataError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    def __init__(self, msg, code=None):
        self.code = code
        self.msg = msg
    
    def __unicode__(self):
        return u'IntegrityError: %s code:%s' % (self.msg, self.code or '')
    
    def __str__(self):
        return str(self.__unicode__())

class ProgrammingError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass

class TooManyConnections(Error):
    pass
