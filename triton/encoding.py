import decimal
import datetime


def msgpack_encode_default(obj):
    """Extra encodings for python types into msgpack

    These are attempted if our normal serialization fails.
    """
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, datetime.datetime):
        return obj.isoformat(' ')
    if isinstance(obj, datetime.date):
        return obj.strftime("%Y-%m-%d")
    try:
        return repr(obj)
    except Exception:
        raise TypeError("Unknown type: %r" % (obj,))
