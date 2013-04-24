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

class RSConnectionError(InterfaceError):
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

class AuthenticationError(Error):
    pass
