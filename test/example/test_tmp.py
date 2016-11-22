# -*- coding: utf-8 -*-
# /home/smokybobo/opt/repos/git/personal/loadlimit/test/unit/stat/test_tmp.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Tempy"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from collections import defaultdict, namedtuple
from concurrent.futures import ThreadPoolExecutor
from functools import partial

# Third-party imports
import pandas as pd
from pandas import DataFrame, Series, Timestamp
import pytest
import uvloop

# Local imports
from loadlimit.core import BaseLoop
from loadlimit.event import NoEventTasksError, recordperiod, shutdown
from loadlimit.stat import timecoro
from loadlimit.util import aiter


# ============================================================================
# Globals
# ============================================================================


pytestmark = pytest.mark.usefixtures('testlogging')


# ============================================================================
# Helpers
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
        ret = sum(len(s) for slist in self.values()
                  for s in slist)
        self.numdata = ret
        return ret

    async def atotal(self):
        """Async total calculator"""
        ret = 0
        async for slist in aiter(self.values()):
            async for s in aiter(slist):
                ret = ret + len(s)
        self.numdata = ret
        return ret

    def dataframe(self, key, startind=0):
        """Create a dataframe from a stored list of series"""
        slist = self[key]
        index = list(range(startind, startind + len(slist)))
        return DataFrame(slist, index=index)

    def clearvals(self, key=None):
        """Clear list of given key

        If key is None, clears list of all keys.

        """
        if key is not None:
            self[key] = []
        else:
            for key in self:
                self[key] = []
        self.numdata = 0

    async def aclearvals(self, key=None):
        """Async version of clearvals()"""
        if key is not None:
            self[key] = []
        else:
            async for key in aiter(self):
                self[key] = []
        self.numdata = 0


def hdf5_results(store, statsdict):
    """Create results from hdf5 store"""
    # Dates
    start = statsdict.start_date
    end = statsdict.end_date

    # Duration (in seconds)
    duration = (end - start).total_seconds()

    results = {}
    index = ['Total', 'Median', 'Average', 'Min', 'Max', 'Rate']
    ResultType = namedtuple('ResultType', [n.lower() for n in index])

    for name in statsdict:
        key = 'timeseries/{}'.format(name)

        # Number of iterations
        storeobj = store.get_storer(key)
        numiter = storeobj.nrows
        df = store[key]
        delta = df['delta']

        r = [numiter]
        for val in [delta.median(), delta.mean(), delta.min(),
                    delta.max()]:
            r.append(val.total_seconds() * 1000)
        r.append(numiter / duration)
        r = ResultType(*r)
        results[name] = Series(r, index=index)

    dfindex = list(sorted(results, key=lambda k: k))
    vals = [results[v] for v in dfindex]
    df = DataFrame(vals, index=dfindex)
    print(df)
    # print(df.info())


def memory_results(statsdict):
    """Create results from hdf5 store"""
    # key = 'timeseries/{}'.format(name)

    # Dates
    start = statsdict.start_date
    end = statsdict.end_date

    # Duration (in seconds)
    duration = (end - start).total_seconds()

    results = {}
    index = ['Total', 'Median', 'Average', 'Min', 'Max', 'Rate']
    ResultType = namedtuple('ResultType', [n.lower() for n in index])

    # Number of iterations
    # storeobj = store.get_storer(key)
    # numiter = storeobj.nrows
    for name, slist in statsdict.items():
        numiter = len(slist)

        df = DataFrame(slist, index=list(range(numiter)))
        delta = df['delta']

        numiter = len(slist)
        r = [numiter]
        for val in [delta.median(), delta.mean(), delta.min(),
                    delta.max()]:
            r.append(val.total_seconds() * 1000)
        r.append(numiter / duration)
        r = ResultType(*r)
        results[name] = Series(r, index=index)

    dfindex = list(sorted(results, key=lambda k: k))
    vals = [results[v] for v in dfindex]
    df = DataFrame(vals, index=dfindex)
    print(df)


async def update_h5(store, statsdict, data):
    """h5"""
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

    # Put memory dict into hdf5 db if it contains 50000 data points
    if statsdict.numdata == 500:
        key = 'timeseries/{}'.format(name)
        # loop = asyncio.get_event_loop()

        # Setup ThreadPoolExecutor
        # execthread = ThreadPoolExecutor(max_workers=3)

        # Get number of rows in db
        storeobj = store.get_storer(key)
        startind = 0 if storeobj is None else storeobj.nrows

        async for k in aiter(statsdict):
            df = statsdict.dataframe(k, startind)
            startind = startind + len(df)
            store.append(key, df)
            # await loop.run_in_executor(execthread, store.append, key, df)
        await statsdict.aclearvals()


async def last_h5_update(store, statsdict, result, **kwargs):
    """Do a last update"""
    if not statsdict:
        return
    loop = asyncio.get_event_loop()

    # Setup ThreadPoolExecutor
    execthread = ThreadPoolExecutor(max_workers=3)

    # Put memory dict into hdf5 db
    async for k in aiter(statsdict):
        key = 'timeseries/{}'.format(k)
        storeobj = store.get_storer(key)
        startind = 0 if storeobj is None else storeobj.nrows
        df = statsdict.dataframe(k, startind)
        startind = startind + len(df)
        await loop.run_in_executor(execthread, store.append, key, df)
    statsdict.clearvals()


# ============================================================================
# Tests
# ============================================================================


@pytest.mark.skipif(True, reason='devel')
def test_tmp(tmpdir):
    """tmp"""
    # Setup uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    # Setup store
    path = str(tmpdir.join('store.h5'))

    stats = Period()

    with pd.HDFStore(path) as store:
        # Add to shutdown event
        shutdown(partial(last_h5_update, store, stats))

        # Create recordperiod anchor that updates stats
        recordperiod(partial(update_h5, store, stats), runfirst=True)

        # Create coro to time
        @timecoro('churn')
        async def churn():
            """docstring for churn"""
            # await asyncio.sleep(0.1)
            await asyncio.sleep(0)

        async def run():
            """run"""
            async for _ in aiter(range(1000)):
                await churn()
            shutdown.set(exitcode=0)

        # Run all the tasks
        with BaseLoop() as main:

            # Start every event, and ignore events that don't have any tasks
            recordperiod.start(ignore=NoEventTasksError, reschedule=True)

            asyncio.ensure_future(run())
            main.start()

        # memory_results(stats)
        hdf5_results(store, stats)


# ============================================================================
#
# ============================================================================
