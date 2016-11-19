# -*- coding: utf-8 -*-
# loadlimit/stat.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Provide objects and functions used to calculate various statistics"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from collections import defaultdict
from functools import wraps

# Third-party imports
import numpy as np

# Local imports
from .event import EventNotStartedError, recordtime, recordperiod
from .util import TZ_UTC, now


# ============================================================================
# Period
# ============================================================================


class Period(defaultdict):
    """Store time series data by key"""

    def __init__(self, *args, **kwargs):
        super().__init__(list, *args, **kwargs)

    def percentile(self, key, p):
        """Calculate given percentile of all values in key"""
        return np.percentile(self[key], p)


# ============================================================================
# timecoro decorator
# ============================================================================


def timecoro(name):
    """Records the start and stop time of the given corofunc"""
    for event in [recordtime, recordperiod]:
        event.__getitem__(name)

    def deco(corofunc):
        """Function to decorate corofunc"""

        @wraps(corofunc)
        async def wrapper(*args, **kwargs):
            """Record start and stop time"""
            start = now(TZ_UTC)
            await corofunc(*args, **kwargs)
            end = now(TZ_UTC)

            delta = end - start
            ms = delta.total_seconds() * 1000
            d = round(ms)

            # Call recordtime and recordperiod events with the start and end
            # times
            recordperiod.set(eventid=name, ignore=EventNotStartedError,
                             start=start, end=end)
            recordtime.set(eventid=name, ignore=EventNotStartedError,
                           delta=int(d))

        return wrapper

    return deco


# ============================================================================
#
# ============================================================================
