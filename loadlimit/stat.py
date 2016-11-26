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
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Timestamp

# Local imports
from .event import EventNotStartedError, MultiEvent, RunFirst
from .util import aiter, Namespace, now


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


# ============================================================================
# Result classes
# ============================================================================


class Result:
    """Calculate result DataFrame from a Period"""

    def __init__(self, statsdict=None):
        self._statsdict = Period() if statsdict is None else statsdict
        self._vals = Namespace()

    def __iter__(self):
        for name, slist in self._statsdict.items():
            yield name, slist

    def __enter__(self):
        """Start calculating the result"""
        statsdict = self.statsdict
        vals = self.vals
        vals.start = statsdict.start_date
        vals.end = statsdict.end_date
        return self

    def __exit__(self, errtype, err, errtb):
        """Finish calculating the result"""
        raise NotImplementedError

    def __call__(self):
        """Calculate the result"""
        calcfunc = self.calculate
        vals = self.vals
        with self:
            for name, slist in self:
                calcfunc(name, slist)

        return vals.results

    def calculate(self, name, slist):
        """Calculate results"""
        raise NotImplementedError

    @property
    def statsdict(self):
        """Return stored period statsdict"""
        return self._statsdict

    @property
    def vals(self):
        """Return value namespace"""
        return self._vals


class Total(Result):
    """Calculate totals"""

    def __enter__(self):
        ret = super().__enter__()
        vals = self.vals

        # Duration (in seconds
        vals.duration = (vals.end - vals.start).total_seconds()
        vals.results = {}

        vals.index = i = ['Total', 'Median', 'Average', 'Min', 'Max', 'Rate']
        vals.resultcls = namedtuple('ResultType', [n.lower() for n in i])
        return ret

    def __exit__(self, errtype, err, errtb):
        """Finish calculations and save result"""
        results = self.vals.results
        dfindex = list(sorted(results, key=lambda k: k))
        data = [results[v] for v in dfindex]
        self.vals.results = DataFrame(data, index=dfindex)

    def calculate(self, name, slist):
        """Calculate results"""
        vals = self.vals

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
        r.append(numiter / vals.duration)
        r = vals.resultcls(*r)
        vals.results[name] = Series(r, index=vals.index)


class TimeSeries(Result):
    """Calculate time series results"""

    def __enter__(self):
        ret = super().__enter__()
        vals = self.vals

        # Dates
        start = vals.start
        end = vals.end

        date_array = np.linspace(start.value, end.value, vals.periods)
        vals.daterange = pd.to_datetime(date_array)

        vals.response_result = {}
        vals.rate_result = {}
        return ret

    def __exit__(self, errtype, err, errtb):
        """Finish calculations and save result"""
        vals = self.vals
        response_result = vals.response_result
        rate_result = vals.rate_result

        # Create response dataframe
        dfindex = list(sorted(response_result, key=lambda k: k))
        data = [response_result[name] for name in dfindex]
        df_response = DataFrame(data, index=dfindex).fillna(0)

        # Create rate dataframe
        data = [rate_result[name] for name in dfindex]
        df_rate = DataFrame(data, index=dfindex).fillna(0)

        # Return both dataframes
        vals.results = (df_response, df_rate)
        for n in ['response_result', 'rate_result']:
            delattr(vals, n)

    def __call__(self, *, periods=1):
        self.vals.periods = periods
        return super().__call__()

    def calculate(self, name, slist):
        """Calculate results"""
        vals = self.vals

        response = []
        rate = []

        # Number of iterations
        numiter = len(slist)

        # Create dataframe out of the timeseries and get average response time
        # for each determined datetime period
        df = DataFrame(slist, index=list(range(numiter)))
        startpoint = vals.start
        for d in vals.daterange:
            d = Timestamp(d, tz='UTC')
            delta = df.query('end > @startpoint and end <= @d')['delta']

            # Average response times
            avg_response = delta.mean().total_seconds() * 1000
            response.append(avg_response)

            # Iterations per second
            duration = (d - startpoint).total_seconds()
            iter_per_sec = 0 if duration <= 0 else len(delta) / duration
            rate.append(iter_per_sec)

            startpoint = d

        daterange = vals.daterange
        vals.response_result[name] = Series(response, index=daterange)
        vals.rate_result[name] = Series(rate, index=daterange)


# ============================================================================
#
# ============================================================================
