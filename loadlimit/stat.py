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
import asyncio
from asyncio import Lock
from copy import deepcopy
from hashlib import sha1
from itertools import count
from collections import defaultdict, namedtuple
from functools import wraps
from itertools import chain
from time import perf_counter

# Third-party imports
import pandas as pd
from pandas import (DataFrame, Series, Timedelta)
from pandas.io import sql

# Local imports
from .channel import AnchorType, DataChannel
from .util import aiter, now


# ============================================================================
# Globals
# ============================================================================


CountStoreData = namedtuple('CountStoreData',
                            ['name', 'end', 'delta', 'rate', 'error',
                             'failure', 'reset', 'clientcount'])


# ============================================================================
# Errors
# ============================================================================


class Failure(Exception):
    """Exception returned by a coroutine to indicate failure"""


# ============================================================================
# Events
# ============================================================================


timedata = DataChannel(name='timedata')


# ============================================================================
# CountStore
# ============================================================================


class Count:
    __slots__ = ('success', 'error', 'failure', 'client')

    def __init__(self):
        self.success = 0
        self.error = defaultdict(lambda: 0)
        self.failure = defaultdict(lambda: 0)
        self.client = set()

    def addsuccess(self):
        """Increment the success count"""
        self.success += 1

    def adderror(self, err):
        """Add an error and increment its count"""
        self.error[err] += 1

    def addfailure(self, fail):
        """Add a failure and increment its count"""
        self.failure[fail] += 1

    def addclient(self, clientid):
        """Increment the client count"""
        self.client.add(clientid)

    def resetclient(self):
        """Set client to a new empty set"""
        self.client = set()

    def sum(self):
        """Sum of all counts

        Does not include client counts.

        """
        return sum(chain([self.success], self.error.values(),
                         self.failure.values()))


class CountStore(defaultdict):
    """Store counts of wrapped coroutines that have completed"""

    def __init__(self):
        super().__init__(Count)
        self.start = None
        self.start_date = None
        self.end = None
        self.end_date = None

    def __deepcopy__(self, memo):
        """Create a deepcopy of this CountStore instance"""
        copy = self.__class__()

        # Copy attributes
        copy.start = deepcopy(self.start, memo)
        copy.start_date = deepcopy(self.start_date, memo)
        copy.end = deepcopy(self.end, memo)
        copy.end_date = deepcopy(self.end_date, memo)

        # Copy dict
        for k, v in self.items():
            copy[k] = deepcopy(v, memo)

        return copy

    def __call__(self, corofunc=None, *, name=None, clientid=None):
        """Decorator that records rate and response time of a corofunc"""
        if name is None:
            raise ValueError('name not given')
        elif not isinstance(name, str):
            msg = ('name expected str, got {} instead'.
                   format(type(name).__name__))
            raise TypeError(msg)

        def deco(corofunc):
            """Function to decorate corofunc"""

            @wraps(corofunc)
            async def wrapper(*args, **kwargs):
                """Measure coroutine runtime"""
                count = self[name]
                start = self.start
                ret = None
                if start is None:
                    self.start_date = now()
                    self.start = start = perf_counter()
                try:
                    ret = await corofunc(*args, **kwargs)
                except Failure as e:
                    failure = str(e.args[0])
                    count.failure[failure] += 1
                except Exception as e:
                    count.error[repr(e)] += 1
                else:
                    count.success += 1
                    count.client.add(clientid)
                finally:
                    self.end = perf_counter()
                    self.end_date = now()
                return ret

            return wrapper

        if corofunc is None:
            return deco

        return deco(corofunc)

    def allresetclient(self):
        """Call resetclient() on every stored Count object"""
        for v in self.values():
            v.resetclient()

    def reset(self):
        """Reset all counts to starting values"""
        self.start = None
        self.start_date = None
        self.end = None
        self.end_date = None
        self.clear()


measure = CountStore()


class SendTimeData:

    stop = False

    def __init__(self, countstore, *, flushwait=None, channel=None):
        if not isinstance(flushwait, (type(None), Timedelta)):
            msg = 'flushwait expected pandas.Timedelta, got {} instead'
            raise TypeError(msg.format(type(flushwait).__name__))

        if not isinstance(channel, (type(None), DataChannel)):
            msg = 'channel expected DataChannel, got {} instead'
            raise TypeError(msg.format(type(channel).__name__))

        self._countstore = countstore
        self._flushwait = (2 if flushwait is None else
                           flushwait.total_seconds())
        self._channel = timedata if channel is None else channel
        self._start = None

    async def __call__(self):
        """Store timedata"""
        wait = self._flushwait
        countstore = self._countstore
        self._start = countstore.start
        snapshot = None
        prevsnapshot = None
        sendfunc = self.send
        prevtime = perf_counter()
        while True:
            await asyncio.sleep(wait)
            if self.stop:
                break
            curtime = perf_counter()
            delta = curtime - prevtime
            snapshot = deepcopy(countstore)
            countstore.allresetclient()
            await sendfunc(delta, snapshot, prevsnapshot)
            prevtime = curtime
            prevsnapshot = snapshot
            if self.stop:
                break

    async def send(self, delta, snapshot, prevsnapshot):
        """Send snapshot diff"""
        mkdata = self.mkdata
        end_date = snapshot.end_date
        channel = self._channel
        reset = False
        if self._start is None:
            self._start = snapshot.start
        elif snapshot.start != self._start:
            reset = True
            self._start = snapshot.start
        async for k, count in aiter(snapshot.items()):
            prevcount = None if prevsnapshot is None else prevsnapshot[k]
            data = await mkdata(delta, end_date, k, count, prevcount,
                                reset=reset)
            await channel.send(data)

    async def mkdata(self, delta, end_date, key, count, prevcount, *,
                     reset=False):
        """Calculate rate and response time"""
        numclient = len(count.client)
        success_diff = (count.success - prevcount.success if prevcount
                        else count.success)
        rate = (success_diff / delta) if delta > 0 else 0
        if end_date is None:
            end_date = now()
        #  response = (1 / rate) if rate > 0 else None

        prev = prevcount.error if prevcount else {}
        error = await self.countdiff(count.error, prev)

        prev = prevcount.failure if prevcount else {}
        failure = await self.countdiff(count.failure, prev)

        data = CountStoreData(name=key, end=end_date, delta=delta, rate=rate,
                              error=error, failure=failure, reset=reset,
                              clientcount=numclient)
        return data

    async def countdiff(self, count, prevcount):
        """Return dictionary with count differences"""
        diff = {}
        async for k, v in aiter(count.items()):
            diff[k] = v - prevcount[k] if k in prevcount else v
        return diff

    async def shutdown(self, *args, **kwargs):
        """Shutdown the coro"""
        self.stop = True
        await self._channel.join()


# ============================================================================
# Period
# ============================================================================


class Period(defaultdict):
    """Store time series data by key"""

    def __init__(self, *, lock=None):
        # Error check lock arg
        if not isinstance(lock, (Lock, type(None))):
            msg = 'lock expected asyncio.Lock, got {} instead'
            raise TypeError(msg.format(type(lock).__name__))

        super().__init__(lambda: defaultdict(list))
        self._lock = Lock() if lock is None else lock
        self.numdata = 0
        counter = namedtuple('counter', 'data error failure')
        self.counter = counter(*[c() for c in [count]*3])
        self.totaldata = next(self.counter.data)
        self.totalerror = next(self.counter.error)
        self.totalfailure = next(self.counter.failure)
        self.start_date = None
        self.end_date = None

    def total(self):
        """Calculate the total number of data points are stored"""
        num = sum(len(datalist) for key in self
                  for datalist in self[key].values())
        self.numdata = num
        return num

    async def atotal(self):
        """Async total calculator"""
        ret = 0
        async for datatype in aiter(self.values()):
            async for datalist in aiter(datatype.values()):
                ret = ret + len(datalist)
                await asyncio.sleep(0)
        self.numdata = ret
        return ret

    def addtimedata(self, key, data):
        """Add timedata"""
        if not isinstance(data, Series):
            msg = ('data expected pandas.Series, got {} instead'.
                   format(type(data).__name__))
            raise TypeError(msg)
        slist = self[key]['data']
        slist.append(data)
        self.numdata = self.numdata + 1
        self.totaldata = next(self.counter.data)

    def adderror(self, key, s):
        """Add exception"""
        if not isinstance(s, Series):
            msg = ('data expected pandas.Series, got {} instead'.
                   format(type(s).__name__))
            raise TypeError(msg)
        self[key]['error'].append(s)
        self.numdata = self.numdata + 1
        self.totalerror = next(self.counter.error)

    def addfailure(self, key, s):
        """Add failure message"""
        if not isinstance(s, Series):
            msg = ('data expected pandas.Series, got {} instead'.
                   format(type(s).__name__))
            raise TypeError(msg)
        self[key]['failure'].append(s)
        self.numdata = self.numdata + 1
        self.totalfailure = next(self.counter.failure)

    def timedata(self, key):
        """Iterate over the given key's timedata"""
        return iter(self[key]['data'])

    def error(self, key):
        """Iterate over the given key's errors"""
        return iter(self[key]['error'])

    def failure(self, key):
        """Iterate over the given key's faiures"""
        return iter(self[key]['failure'])

    def numtimedata(self, key):
        """Return the number of errors stored by the given key"""
        return len(self[key]['data'])

    def numerror(self, key):
        """Return the number of errors stored by the given key"""
        return len(self[key]['error'])

    def numfailure(self, key):
        """Return the number of errors stored by the given key"""
        return len(self[key]['failure'])

    def clearvals(self, key=None):
        """Clear list of given key

        If key is None, clears list of all keys.

        """
        genkey = (k for k in [key]) if key is not None else (k for k in self)
        for k in genkey:
            for datalist in self[k].values():
                datalist.clear()

        # This automatically sets numdata to the correct value
        self.total()

    async def aclearvals(self, key=None):
        """Async version of clearvals()"""
        genkey = (k for k in [key]) if key is not None else (k for k in self)
        async for k in aiter(genkey):
            async for datalist in aiter(self[k].values()):
                datalist.clear()
                await asyncio.sleep(0)

        # This automatically sets numdata to the correct value
        await self.atotal()

    @property
    def lock(self):
        """Retrieve the dict's lock"""
        return self._lock


# ============================================================================
# Update stats coroutine
# ============================================================================


@timedata(anchortype=AnchorType.first)
async def updateperiod(data, *, statsdict=None, **kwargs):
    """Update a period/defaultdict(list) with period data point

    This is the anchor coro func for the timedata event.

    """
    name = data.name
    end = data.end
    delta = data.delta
    rate = data.rate
    error = data.error
    failure = data.failure
    reset = data.reset
    clientcount = data.clientcount
    with (await statsdict.lock):
        if reset:
            await statsdict.aclearvals()

        # In-memory dict
        if error:
            async for k, c in aiter(error.items()):
                if c == 0:
                    continue
                error_rate = (c / delta) if delta > 0 else 0
                error_response = (0 if error_rate == 0
                                  else (1 / error_rate) * 1000)
                se = Series([end, error_rate, error_response, k, c],
                            index=['end', 'rate', 'response', 'error',
                                   'count'])
                statsdict.adderror(name, se)
        if failure:
            async for k, c in aiter(failure.items()):
                if c == 0:
                    continue
                failure_rate = (c / delta) if delta > 0 else 0
                failure_response = (0 if failure_rate == 0
                                    else (1 / failure_rate) * 1000)
                sf = Series([end, failure_rate, failure_response, k, c],
                            index=['end', 'rate', 'response', 'failure',
                                   'count'])
                statsdict.addfailure(name, sf)

        clientrate = rate / clientcount if clientcount > 0 else 0
        response = (1 / clientrate) * 1000 if clientrate > 0 else 0
        if rate or response:
            s = Series([end, rate, response],
                       index=['end', 'rate', 'response'])
            statsdict.addtimedata(name, s)


class FlushToSQL:
    """Flush statsdict data to sql db"""

    async def __call__(self, data, *, statsdict=None, sqlengine=None,
                       flushlimit=50000, sqltbl='period', **kwargs):
        with (await statsdict.lock):
            if statsdict.numdata < flushlimit:
                return

            with sqlengine.begin() as conn:
                async for k in aiter(statsdict):
                    # Generate table name
                    curkey = sha1(k.encode('utf-8')).hexdigest()
                    for n in ['flushdata', 'flusherror', 'flushfailure']:
                        func = getattr(self, n)
                        func(statsdict, k, sqltbl, curkey, sqlengine, conn)

            await statsdict.aclearvals()

    def countrows(self, sqlengine, sqlconn, tblname):
        """Retrieve number of rows from the given table"""
        # Get number of rows in db
        hastable = sqlengine.dialect.has_table(sqlengine, tblname)
        qry = 'SELECT count(*) FROM {}'.format(tblname)
        numrows = (0 if not hastable else
                   sql.execute(qry, sqlconn).fetchone()[0])
        return numrows

    def mktimeseries(self, data):
        """Create list of timeseries data"""
        to_datetime = pd.to_datetime
        slist = []
        for s in data:
            converted = list(s)
            val = [to_datetime(s[0].value)]
            val.extend(converted[1:])
            slist.append(val)
        return slist

    def flushdata(self, statsdict, key, sqltbl, namekey, sqlengine, sqlconn):
        """Flush time data"""
        curname = '{}_{}'.format(sqltbl, namekey)
        startind = self.countrows(sqlengine, sqlconn, curname)

        # Convert each series to use naive datetimes and nanosecond
        # int/float values instead of timedeltas
        slist = self.mktimeseries(statsdict.timedata(key))
        if not slist:
            return
        index = list(range(startind, startind + len(slist)))
        df = DataFrame(slist, index=index,
                       columns=['end', 'rate', 'response'])
        startind = startind + len(df)
        df.to_sql(curname, sqlconn, if_exists='append')

    def flusherror(self, statsdict, key, sqltbl, namekey, sqlengine, sqlconn):
        """Flush error data"""
        slist = self.mktimeseries(statsdict.error(key))
        if not slist:
            return
        curname = '{}_error_{}'.format(sqltbl, namekey)
        startind = self.countrows(sqlengine, sqlconn, curname)

        index = list(range(startind, startind + len(slist)))
        df = DataFrame(slist, index=index,
                       columns=['end', 'rate', 'response', 'error', 'count'])
        startind = startind + len(df)
        df.to_sql(curname, sqlconn, if_exists='append')

    def flushfailure(self, statsdict, key, sqltbl, namekey, sqlengine,
                     sqlconn):
        """Flush error data"""
        slist = list(self.mktimeseries(statsdict.failure(key)))
        if not slist:
            return
        curname = '{}_failure_{}'.format(sqltbl, namekey)
        startind = self.countrows(sqlengine, sqlconn, curname)

        index = list(range(startind, startind + len(slist)))
        df = DataFrame(slist, index=index,
                       columns=['end', 'rate', 'response', 'failure', 'count'])
        startind = startind + len(df)
        df.to_sql(curname, sqlconn, if_exists='append')


flushtosql = FlushToSQL()


async def flushtosql_shutdown(exitcode, *, statsdict=None, sqlengine=None,
                              sqltbl='period', **kwargs):
    """Flush statsdict to sql on shutdown"""
    with (await statsdict.lock):
        if statsdict.numdata == 0:
            return

    await flushtosql(None, statsdict=statsdict, sqlengine=sqlengine,
                     flushlimit=0, sqltbl=sqltbl)


# ============================================================================
#
# ============================================================================
