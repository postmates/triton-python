import io
import yaml
import logging

from . import errors

log = logging.getLogger(__name__)

REQUIRED_CONFIG_KEYS = ['name', 'partition_key']


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
