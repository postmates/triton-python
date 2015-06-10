# -*- coding: utf-8 -*-

"""
This module contains the set of triton's exceptions

:copyright: (c) 2012 by Firstname Lastname.
:license: ISC, see LICENSE for more details.

"""


class Error(Exception):
    """This is an ambiguous error that occured."""
    pass


class ShardNotFoundError(Error):
    """Indicates the requested shard isn't known"""
    pass


class EndOfShardError(Error):
    pass
