import io
import yaml
import logging
import os

from . import errors

ENV_VAR_TRITON_ZMQ_HOST = 'TRITON_ZMQ_HOST'
ENV_VAR_TRITON_ZMQ_PORT = 'TRITON_ZMQ_PORT'
ZMQ_DEFAULT_HOST = '127.0.0.1'
ZMQ_DEFAULT_PORT = 3515

log = logging.getLogger(__name__)

REQUIRED_CONFIG_KEYS = ['name', 'partition_key']

_zmq_config = None


def load_config(file_name):
    try:
        config_dict = yaml.load(io.open(file_name))
    except IOError as e:
        log.error("Failed to open %s: %r", file_name, e)
        return None

    for stream_name, v in config_dict.iteritems():
        for k in REQUIRED_CONFIG_KEYS:
            if k not in v:
                raise errors.InvalidConfigurationError(
                    "Missing {} : {}".format(stream_name, k))

    return config_dict


def get_zmq_config():
    global _zmq_config
    if not _zmq_config:
        zmq_host = os.environ.get(ENV_VAR_TRITON_ZMQ_HOST, ZMQ_DEFAULT_HOST)
        zmq_port = os.environ.get(ENV_VAR_TRITON_ZMQ_PORT, ZMQ_DEFAULT_PORT)
        _zmq_config = (zmq_host, zmq_port)

    return _zmq_config
