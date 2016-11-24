# -*- coding: utf-8 -*-
# loadlimit/coro.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Useful utility coroutines."""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from asyncio import sleep
from datetime import timedelta

# Third-party imports

# Local imports
from .util import now, TZ_UTC
from . import event


# ============================================================================
# Exceptions
# ============================================================================


class NotUTCError(Exception):
    """Error raised if a datetime does not have a UTC timezone"""


# ============================================================================
# Wait until given datetime
# ============================================================================


async def sleep_until(end, future=None, **kwargs):
    """Sleep until the given datetime given via ``dt``

    If end is the same as or before the current datetime, this is considered
    the same as not sleeping at all.

    Note: Raises NotUTCError if ``end`` does not have a UTC timezone.

    """
    if end.tzinfo != TZ_UTC:
        raise NotUTCError

    start = now()
    delta = end - start
    period = round(delta.total_seconds())
    if period < 0:
        period = 0
    await sleep(period)

    if future is not None:
        future.set_result(kwargs)


async def wait_until(end, future=None, **kwargs):
    """Wait until the given datetime.

    Similar to sleep_until, but this will check the time once every hour until
    the duration to the desired endtime is less than an hour. Once that
    happens, it will wait for the rest of the amount of time until the desired
    date time is reached.

    """
    if end.tzinfo != TZ_UTC:
        raise NotUTCError

    hour_seconds = 3600
    current = None
    while True:
        current = now()
        delta = end - current
        period = round(delta.total_seconds())
        if period < 0:
            period = 0

        if period < hour_seconds:
            await sleep(period)
            break
        elif period == hour_seconds:
            await sleep(hour_seconds)
            break

        # period > hour_seconds
        await sleep(hour_seconds)
        continue

    if future is not None:
        future.set_result(kwargs)


async def maxruntime(delta, *, future=None, **kwargs):
    """Set shutdown event once timedelta delta has passed"""
    if not isinstance(delta, timedelta):
        msg = 'delta expected timedelta, got {} instead'
        raise TypeError(msg.format(type(delta).__name__))
    current = now()
    end_date = current + delta
    await wait_until(end_date, future=future, **kwargs)
    event.shutdown.set(exitcode=0)


# ============================================================================
#
# ============================================================================
