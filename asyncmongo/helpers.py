import hashlib

import bson
from bson.son import SON
import struct
from asyncmongo import ASCENDING, DESCENDING, GEO2D
from asyncmongo.errors import (DatabaseError, InterfaceError)


def _parse_host(h):
    try:
        host, port = h.split(":", 1)
        port = int(port)
    except ValueError:
        raise ValueError("Wrong host:port value: %s" % h)

    return host, port

def _unpack_response(response, cursor_id=None, as_class=dict, tz_aware=False):
    """Unpack a response from the database.

    Check the response for errors and unpack, returning a dictionary
    containing the response data.

    :Parameters:
      - `response`: byte string as returned from the database
      - `cursor_id` (optional): cursor_id we sent to get this response -
        used for raising an informative exception when we get cursor id not
        valid at server response
      - `as_class` (optional): class to use for resulting documents
    """
    response_flag = struct.unpack("<i", response[:4])[0]
    if response_flag & 1:
        # Shouldn't get this response if we aren't doing a getMore
        assert cursor_id is not None

        raise InterfaceError("cursor id '%s' not valid at server" %
                               cursor_id)
    elif response_flag & 2:
        error_object = bson.BSON(response[20:]).decode()
        if error_object["$err"] == "not master":
            raise DatabaseError("master has changed")
        raise DatabaseError("database error: %s" %
                               error_object["$err"])

    result = {}
    result["cursor_id"] = struct.unpack("<q", response[4:12])[0]
    result["starting_from"] = struct.unpack("<i", response[12:16])[0]
    result["number_returned"] = struct.unpack("<i", response[16:20])[0]
    result["data"] = bson.decode_all(response[20:], as_class, tz_aware)
    assert len(result["data"]) == result["number_returned"]
    return result

def _fields_list_to_dict(fields):
    """Takes a list of field names and returns a matching dictionary.

    ["a", "b"] becomes {"a": 1, "b": 1}

    and

    ["a.b.c", "d", "a.c"] becomes {"a.b.c": 1, "d": 1, "a.c": 1}
    """
    for key in fields:
        assert isinstance(key, (str,unicode))
    return dict([[key, 1] for key in fields])

def _index_document(index_list):
    """Helper to generate an index specifying document.

    Takes a list of (key, direction) pairs.
    """
    if isinstance(index_list, dict):
        raise TypeError("passing a dict to sort/create_index/hint is not "
                        "allowed - use a list of tuples instead. did you "
                        "mean %r?" % list(index_list.iteritems()))
    elif not isinstance(index_list, list):
        raise TypeError("must use a list of (key, direction) pairs, "
                        "not: " + repr(index_list))
    if not len(index_list):
        raise ValueError("key_or_list must not be the empty list")

    index = SON()
    for (key, value) in index_list:
        if not isinstance(key, basestring):
            raise TypeError("first item in each key pair must be a string")
        if value not in [ASCENDING, DESCENDING, GEO2D]:
            raise TypeError("second item in each key pair must be ASCENDING, "
                            "DESCENDING, or GEO2D")
        index[key] = value
    return index

def _password_digest(username, password):
    """Get a password digest to use for authentication.
    """
    if not isinstance(password, basestring):
        raise TypeError("password must be an instance of basestring")
    if not isinstance(username, basestring):
        raise TypeError("username must be an instance of basestring")

    md5hash = hashlib.md5()
    md5hash.update("%s:mongo:%s" % (username.encode('utf-8'),
                                    password.encode('utf-8')))
    return unicode(md5hash.hexdigest())

def _auth_key(nonce, username, password):
    """Get an auth key to use for authentication.
    """
    digest = _password_digest(username, password)
    md5hash = hashlib.md5()
    md5hash.update("%s%s%s" % (nonce, unicode(username), digest))
    return unicode(md5hash.hexdigest())
