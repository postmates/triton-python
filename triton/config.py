from __future__ import unicode_literals
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

REQUIRED_CONFIG_KEYS_V1 = ['name', 'partition_key']

PROVIDER_GCP = 'gcp'
PROVIDER_AWS = 'aws'
REQUIRED_CONFIG_KEYS_V2 = ['provider']
REQUIRED_CONFIG_VALID_PROVIDERS_V2 = [PROVIDER_AWS, PROVIDER_GCP]
REQUIRED_CONFIG_KEYS_AWS_V2 = REQUIRED_CONFIG_KEYS_V1
REQUIRED_CONFIG_KEYS_GCP_V2 = ['project', 'topic', 'private_key_file']

_zmq_config = None

def _validate_config(stream_name, config, required):
    for k in required:
        if k not in config:
            raise errors.InvalidConfigurationError(
                "Missing {} : {}".format(stream_name, k))
       
#NOTE: when loading config dictionary, yaml automatically converts unicode
#in the yaml file to unicode in the dictionary. Dictionary will still contain
#ascii keys and values for ascii strings in the yaml file
def load_config(file_name):
    try:
        config_dict = yaml.load(io.open(file_name))
    except IOError as e:
        log.error("Failed to open %s: %r", file_name, e)
        return None

    for stream_name, stream_config in config_dict.iteritems():
        provider = stream_config.get('provider', PROVIDER_AWS)
        if provider not in REQUIRED_CONFIG_VALID_PROVIDERS_V2:
            raise errors.InvalidConfigurationError(
                "Invalid provider.  Supported providers = {}".format(REQUIRED_CONFIG_VALID_PROVIDERS_V2))

        required_keys = REQUIRED_CONFIG_KEYS_AWS_V2 if provider == PROVIDER_AWS else REQUIRED_CONFIG_KEYS_GCP_V2
        _validate_config(stream_name, stream_config, required_keys)

    return dict(provider=provider, **config_dict)


def get_zmq_config():
    global _zmq_config
    if not _zmq_config:
        zmq_host = os.environ.get(ENV_VAR_TRITON_ZMQ_HOST, ZMQ_DEFAULT_HOST)
        zmq_port = os.environ.get(ENV_VAR_TRITON_ZMQ_PORT, ZMQ_DEFAULT_PORT)
        _zmq_config = (zmq_host, zmq_port)

    return _zmq_config
