# -*- coding: utf-8 -*-
"""
triton.nonblocking_stream
~~~~~~~~

This module provides for a way to write to a Triton Stream without
blocking to wait for a successful write to the stream.
It does this by sending the messageover ZeroMQ to tritond.

Adapted from https://github.com/rhettg/BlueOx/blob/master/blueox/network.py

"""
import logging
import threading
import struct
import atexit

import zmq
import msgpack

from . import errors
from . import config

log = logging.getLogger(__name__)

# We want to limit how many messages we'll hold in memory so if our tritond is
# unavailable, we don't just run out of memory.  I based this value on rough
# value of rather large 3k sized messages, and how many we can fit in 10 megs.
MAX_QUEUED_MESSAGES = 3500

# If we have pending outgoing messages, this is how long we'll wait after
# being told to exit.
LINGER_SHUTDOWN_MSECS = 3000

META_STRUCT_FMT = "!B64p64p"

# We're going to include a version byte in our meta struct for future
# upgrading.  This is very quickly getting into the kind of bit packing I
# wanted to avoid for a logging infrastructure, but the performance gain is
# hard to ignore.
META_STRUCT_VERSION = 0x4


def check_meta_version(meta):
    value, = struct.unpack(">B", meta[0])
    if value != META_STRUCT_VERSION:
        raise ValueError(value)


threadLocal = threading.local()

# Context can be shared between threads
_zmq_context = None
_connect_str = None


def init(host, port):
    global _zmq_context
    global _connect_str

    _zmq_context = zmq.Context()
    _connect_str = "tcp://%s:%d" % (host, port)


def _thread_connect():
    if _zmq_context and not getattr(threadLocal, 'zmq_socket', None):
        threadLocal.zmq_socket = _zmq_context.socket(zmq.PUSH)
        threadLocal.zmq_socket.hwm = MAX_QUEUED_MESSAGES
        threadLocal.zmq_socket.linger = LINGER_SHUTDOWN_MSECS

        threadLocal.zmq_socket.connect(_connect_str)


class NonblockingStream(object):

    def __init__(self, name, partition_key):
        self.name = name
        if len(self.name) > 64:
            raise ValueError("Stream Name Too Long")
        self.partition_key = partition_key
        if _zmq_context is None:
            init(*config.get_zmq_config())

    def _partition_key(self, data):
        return str(data[self.partition_key])

    def _serialize_context(self, data):
        # Our sending format is made up of two messages. The first has a
        # quick to unpack set of meta data that our collector is going to
        # use for routing and stats. This is much faster than having the
        # collector decode the whole event. We're just going to use python
        # struct module to make a quick and dirty data structure
        if len(self._partition_key(data)) > 64:
            raise ValueError("Partition Key Too Long")

        meta_data = struct.pack(META_STRUCT_FMT, META_STRUCT_VERSION,
                                self.name,
                                self._partition_key(data))

        message_data = msgpack.packb(data)

        return meta_data, message_data

    def put(self, data):
        global _zmq_context
        _thread_connect()

        try:
            meta_data, message_data = self._serialize_context(data)
        except Exception:
            log.exception(
                "Triton serialization failure for stream {}".format(
                    self.name))
            return

        if _zmq_context and threadLocal.zmq_socket is not None:
            try:
                log.debug("Sending msg")
                threadLocal.zmq_socket.send_multipart(
                    (meta_data, message_data), zmq.NOBLOCK)
            except zmq.ZMQError:
                log.exception(
                    ("Failed sending Triton event over ZMQ. ",
                        "Buffer full? tritond not running?"))
        else:
            log.info("Skipping sending event %s", self.name)


def get_nonblocking_stream(stream_name, config):
    s_config = config.get(stream_name)
    if not s_config:
        raise errors.StreamNotConfiguredError()

    return NonblockingStream(stream_name, s_config['partition_key'])


def close():
    global _zmq_context

    if getattr(threadLocal, 'zmq_socket', None):
        threadLocal.zmq_socket.close()
        threadLocal.zmq_socket = None

    _zmq_context = None


atexit.register(close)
