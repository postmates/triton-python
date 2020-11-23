from __future__ import unicode_literals
import decimal
import datetime
import six


def msgpack_encode_default(obj):
    """Extra encodings for python types into msgpack

    These are attempted if our normal serialization fails.
    """
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, datetime.datetime):
        return obj.isoformat(str(' '))
    if isinstance(obj, datetime.date):
        return obj.strftime("%Y-%m-%d")
    if hasattr(obj, 'coords'):
        # hack to deal with lat-long points
        return repr(obj.coords)
    try:
        return repr(obj)
    except Exception:
        raise TypeError("Unknown type: %r" % (obj,))


def unicode_to_ascii_str(text):
    # if unicode, escape out multibyte characters
    if text is None:
        return b''

    #if unicode, escape out multibyte characters
    if isinstance(text, six.text_type):
        return text.encode('utf-8')
    # need to str here because this function could be fed something
    # that's not unicode but needs to be an ascii string (e.g. an int)
    return six.b(text)


def ascii_to_unicode_str(text):
    # if ascii/escaped unicode, decode to utf-8
    if isinstance(text, six.binary_type):
        return text.decode('utf-8')
    # need to unicode here because this function could be fed something
    # that's not an ascii str but needs to be a unicode string (e.g. an int)
    if six.PY2:
        return six.u(text)
    return str(text)
