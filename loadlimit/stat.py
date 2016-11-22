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
from collections import defaultdict, namedtuple
from functools import wraps

# Third-party imports
from pandas import DataFrame, Series, Timestamp

# Local imports
from .event import EventNotStartedError, MultiEvent, RunFirst
from .util import aiter, now


# ============================================================================
# Events
# ============================================================================


recordperiod = MultiEvent(RunFirst)


# ============================================================================
# Period
# ============================================================================


class Period(defaultdict):
    """Store time series data by key"""

    def __init__(self, *args, **kwargs):
        super().__init__(list, *args, **kwargs)
        self.numdata = 0
        self.start_date = None
        self.end_date = None

    def total(self):
        """Calculate the total number of data points are stored"""
        ret = sum(len(slist) for slist in self.values())
        self.numdata = ret
        return ret

    async def atotal(self):
        """Async total calculator"""
        ret = 0
        async for slist in aiter(self.values()):
            ret = ret + len(slist)
        self.numdata = ret
        return ret

    def clearvals(self, key=None):
        """Clear list of given key

        If key is None, clears list of all keys.

        """
        if key is not None:
            self[key] = []
        else:
            for key in self:
                self[key] = []

        # This automatically sets numdata to the correct value
        self.total()

    async def aclearvals(self, key=None):
        """Async version of clearvals()"""
        if key is not None:
            self[key] = []
        else:
            async for key in aiter(self):
                self[key] = []

        # This automatically sets numdata to the correct value
        await self.atotal()


# ============================================================================
# timecoro decorator
# ============================================================================


def timecoro(name):
    """Records the start and stop time of the given corofunc"""
    recordperiod.__getitem__(name)

    def deco(corofunc):
        """Function to decorate corofunc"""

        @wraps(corofunc)
        async def wrapper(*args, **kwargs):
            """Record start and stop time"""
            start = now()
            await corofunc(*args, **kwargs)
            end = now()

            # Call recordperiod event with the start and end times
            recordperiod.set(eventid=name, ignore=EventNotStartedError,
                             start=start, end=end, callclear=True)

        return wrapper

    return deco


# ============================================================================
# Update stats coroutine
# ============================================================================


@recordperiod(runfirst=True)
async def updateperiod(data, *, statsdict=None):
    """Update a period/defaultdict(list) with period data point

    This is the anchor coro func for the recordperiod event.

    """
    name = data.eventid
    start, end = Timestamp(data.start), Timestamp(data.end)
    delta = end - start
    if statsdict.start_date is None:
        statsdict.start_date = start
    statsdict.end_date = end
    s = Series([start, end, delta], index=['start', 'end', 'delta'])

    # In-memory dict
    slist = statsdict[name]
    slist.append(s)
    statsdict.numdata = statsdict.numdata + 1


def periodresults(statsdict):
    """Create results from period statsdict"""
    # Dates
    start = statsdict.start_date
    end = statsdict.end_date

    # Duration (in seconds)
    duration = (end - start).total_seconds()

    results = {}
    index = ['Total', 'Median', 'Average', 'Min', 'Max', 'Rate']
    ResultType = namedtuple('ResultType', [n.lower() for n in index])

    for name, slist in statsdict.items():

        # Number of iterations
        numiter = len(slist)

        # Create dataframe out of the timeseries and get only the delta field
        df = DataFrame(slist, index=list(range(numiter)))
        delta = df['delta']

        # Calculate stats
        r = [numiter]
        for val in [delta.median(), delta.mean(), delta.min(),
                    delta.max()]:
            r.append(val.total_seconds() * 1000)
        r.append(numiter / duration)
        r = ResultType(*r)
        results[name] = Series(r, index=index)

    # Create result dataframe
    dfindex = list(sorted(results, key=lambda k: k))
    vals = [results[v] for v in dfindex]
    df = DataFrame(vals, index=dfindex)
    return df


# ============================================================================
#
# ============================================================================
