# -*- coding: utf-8 -*-
"""
triton
~~~~~~~~

:copyright: (c) 2015 by Postmates
"""

__title__ = 'triton'
__version__ = '0.0.4'
__description__ = 'Triton - Data Pipeline'
__url__ = 'https://github.com/postmates/postal-triton'
__build__ = 0
__author__ = 'Postmates, Inc'
__author_email__ = 'rhett@postmates.com'
__license__ = 'Private'
__copyright__ = 'Copyright 2015 Postmates'

from .config import load_config
from .stream import get_stream
from .store import stream_from_s3_store
