
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
    pass

class InternalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass
