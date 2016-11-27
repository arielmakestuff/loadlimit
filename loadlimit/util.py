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
from asyncio import iscoroutinefunction
from enum import Enum
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
# Coroutine checks
# ============================================================================


def iscoroutinecallable(obj):
    """Check if obj is a coroutine callable"""
    if not callable(obj):
        return False
    if iscoroutinefunction(obj):
        return True
    callfunc = getattr(obj, '__call__', None)
    return iscoroutinefunction(callfunc)


# ============================================================================
#
# ============================================================================
