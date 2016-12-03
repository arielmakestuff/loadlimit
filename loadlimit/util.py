# -*- coding: utf-8 -*-
# loadlimit/util.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Utility objects and functions"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import argparse
from collections import ChainMap
from enum import Enum
from functools import partial
import logging

# Third-party imports
from pandas import Timestamp
from pytz import UTC

# Local imports


# ============================================================================
# Globals
# ============================================================================


LogLevel = Enum('LogLevel', [(k, v) for k, v in logging._nameToLevel.items()
                             if k not in ['WARN', 'NOTSET']])


TZ_UTC = UTC


# ============================================================================
# Date utils
# ============================================================================


def now(tzinfo=None):
    """Generate the current datetime.

    Defaults to UTC timezone.

    """
    tzinfo = 'UTC' if tzinfo is None else tzinfo
    return Timestamp.now(tz=tzinfo)


# ============================================================================
# Namespace
# ============================================================================


class Namespace(argparse.Namespace):
    """Namespace extended with bool check

    The bool check is to report whether the namespace is empty or not

    """

    def __bool__(self):
        """Return True if attributes are being stored"""
        return self != self.__class__()


# ============================================================================
# Async Iterator
# ============================================================================


class AsyncIterator:
    """Async wrapper around a non-async iterator"""

    def __init__(self, obj):
        self._it = iter(obj)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            value = next(self._it)
        except StopIteration:
            raise StopAsyncIteration
        return value

aiter = AsyncIterator


# ============================================================================
# Logger
# ============================================================================


class Logger:
    """Help make logging easier"""
    __slots__ = ('_logger', '_kwargs', '_lognames')

    def __init__(self, *, logger=None, name=None):
        if name is None:
            name = __name__.partition('.')[0]
        self._logger = (logging.getLogger(name) if logger is None
                        else logger)
        self._kwargs = {}
        self._lognames = frozenset(l.name.lower() for l in LogLevel)

    def __getattr__(self, name):
        """Return a logger method"""
        if name not in self._lognames:
            raise ValueError('Unknown log function name: {}'.format(name))
        l = getattr(LogLevel, name.upper())
        return partial(self.log, l)

    def log(self, level, message, *args, exc_info=None, **kwargs):
        """Log message at the given level"""
        if not isinstance(level, LogLevel):
            msg = ('level expected LogLevel, got {} instead'.
                   format(type(level).__name__))
            raise TypeError(msg)
        kwargs = ChainMap(kwargs, self._kwargs)
        logger = self._logger
        func = getattr(logger, level.name.lower())
        func = func if exc_info is None else partial(func, exc_info=exc_info)
        msg = message.format(*args, **kwargs)
        func(msg)

    @property
    def msgkwargs(self):
        """Update message kwargs"""
        return self._kwargs

    @property
    def logger(self):
        """Return the underlying logger object"""
        return self._logger


# ============================================================================
#
# ============================================================================
