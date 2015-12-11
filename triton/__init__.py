# -*- coding: utf-8 -*-
"""
triton
~~~~~~~~

:copyright: (c) 2015 by Postmates
"""

__title__ = 'triton'
__version__ = '0.0.7'
__description__ = 'Triton - Kinesis Data Pipeline'
__url__ = 'https://github.com/postmates/triton-python'
__build__ = 0
__author__ = 'Postmates, Inc'
__author_email__ = 'rhettg@gmail.com'
__license__ = 'ISC'
__copyright__ = 'Copyright 2015 Postmates'

from .config import load_config
from .stream import get_stream
from .nonblocking_stream import get_nonblocking_stream
from .store import stream_from_s3_store
