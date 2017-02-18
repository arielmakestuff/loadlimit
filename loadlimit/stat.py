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
from collections import defaultdict, namedtuple
from copy import deepcopy
from functools import partial, wraps
from hashlib import sha1
from itertools import chain, count
from time import perf_counter

# Third-party imports
import pandas as pd
from pandas import (DataFrame, Series, Timedelta)
from pandas.io import sql

# Local imports
from .channel import AnchorType, DataChannel
from .util import ageniter, now


# ============================================================================
# Globals
# ============================================================================


CountStoreData = namedtuple('CountStoreData',
                            ['name', 'end', 'delta', 'rate', 'error',
                             'failure', 'reset', 'clientcount'])


ErrorMessage = namedtuple('ErrorMessage', 'error failure')


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
# Frame
# ============================================================================


class Counter:
    __slots__ = ('name', 'value', 'default')

    def __init__(self, name, default=0):
        self.name = name
        self.value = default
        self.default = default

    def increment(self, step):
        """Increment the counter"""
        self.value += step

    def reset(self):
        """Reset the counter to value"""
        self.value = self.default


class ErrorCounter(Counter):
    __slots__ = ()

    def __init__(self, name, default=None):
        super().__init__(name, default)
        self.reset()

    def __getattr__(self, name):
        return getattr(self.value, name)

    def increment(self, errmsg, step):
        """Increment the error counter"""
        self.value[errmsg] += step

    def reset(self):
        """Reset the counter"""
        if self.default is None:
            default = partial(Counter, self.name)
        retval = default()
        if not isinstance(retval, Counter):
            errmsg = ('default function return value expected {} object, '
                      'got {} object instead'.
                      format(Counter.__name__, type(retval).__name__))
            raise TypeError(errmsg)
        self.value = defaultdict(default)


class Frame:
    __slots__ = ('_count', )

    def __init__(self, *counters):
        CountTuple = namedtuple('CountTuple', [c.name for c in counters])
        self._count = CountTuple(*counters)

    def __getattr__(self, name):
        return getattr(self._count, name)

    def reset(self):
        """Call reset on all counters"""
        for c in self._count:
            c.reset()


class LoadFrame(Frame):
    __slots__ = ('start', 'end', 'client')

    def __init__(self, start=None, end=None):
        self.start = start
        self.end = end
        self.client = set()
        success = Counter('success')
        error = ErrorCounter('error')
        failure = ErrorCounter('failure')
        super().__init__(success, error, failure)

    def sum(self):
        """Calculate the sum of all counters"""
        return sum(chain([self.success.value], self.error.values(),
                         self.failure.values()))

    def reset(self):
        """Call reset on all counters"""
        self.client = set()
        super().reset()


class TimelineFrame(LoadFrame):
    __slots__ = ('frame', )

    def __init__(self):
        self.frame = self.newframe()
        super().__init__()

    @staticmethod
    def newframe(start=None, end=None):
        """Create a new frame"""
        return LoadFrame(start=start, end=end)

    # def addframe(self, frame):
    #     """Add a frame to the timeline"""
    #     if not isinstance(frame, LoadFrame):
    #         errmsg = ('frame expected {} object, got {} object instead'.
    #                   format(LoadFrame.__name__, type(frame).__name__))
    #         raise TypeError(errmsg)

    #     start = frame.start
    #     if start is None:
    #         frame.start = start = perf_counter()
    #     self.timeline[start] = frame

    # def popframe(self, framestart):
    #     """Remove and return the frame that started at framestart"""
    #     # Pop current frame
    #     return self.timeline.pop(framestart)

    def addsuccess(self, val=1):
        """Increment the success count"""
        self.success.increment(val)
        self.frame.success.increment(val)

    def adderror(self, err, val=1):
        """Add an error and increment its count"""
        self.error.increment(err, val)
        self.frame.error.increment(err, val)

    def addfailure(self, fail, val=1):
        """Add a failure and increment its count"""
        self.failure.increment(fail, val)
        self.frame.failure.increment(fail, val)

    def addclient(self, clientid):
        """Increment the client count"""
        self.client.add(clientid)

    def resetclient(self):
        """Set client to a new empty set"""
        curframe = self.frame
        curframe.reset()
        curframe.end = None
        curframe.start = perf_counter()
        # timeline = self.timeline
        # if timeline:
        #     key = next(iter(timeline))
        #     curframe.start = timeline[key].start
        # else:
        #     curframe.start = None

    def update(self, old):
        """Add counts of old to timeline counts"""
        # Update client ids
        self.client.update(old.client)

        # Update success
        self.addsuccess(old.success)

        # Dot optimizations
        adderror = self.adderror
        addfailure = self.addfailure

        # Update errors and failures
        for errtxt, val in old.error.items():
            adderror(errtxt, val)

        # Update failures
        for failtxt, val in old.failure.items():
            addfailure(failtxt, val)

    # def oldest(self):
    #     """Get the oldest frame compared to frame started at framestart"""
    #     timeline = self.timeline

    #     # Get oldest frame key
    #     key = next(iter(timeline))
    #     return key, timeline[key]


class CoroMonitor:
    __slots__ = ('timeline', 'corofunc', 'name', 'clientid', 'errors',
                 'curstart')

    def __init__(self, timeline, corofunc, name=None, clientid=None):
        self.timeline = timeline
        self.corofunc = corofunc
        self.name = name
        self.clientid = clientid
        self.errors = ErrorMessage(None, None)
        self.curstart = None

    def __enter__(self):
        timeline = self.timeline
        name = self.name

        window = timeline[name]
        start = timeline.start
        self.curstart = curstart = perf_counter()

        # # Add a new frame
        # frame = window.newframe(start=curstart)
        # window.addframe(frame)

        # Set the window's start time
        framestart = window.frame.start
        if framestart is None or curstart < framestart:
            window.frame.start = curstart

        # Set timeline's start time
        if start is None:
            timeline.start_date = now()
            timeline.start = start = curstart

    def __exit__(self, exctype, exc, exctb):
        error, failure = self.errors
        timeline = self.timeline
        frame = timeline[self.name]
        # curstart = self.curstart

        # # Get oldest frame
        # key, oldframe = window.oldest()

        # # Remove current frame from window
        # window.popframe(curstart)

        # Update oldest frame
        if failure is not None:
            frame.failure.increment(failure, 1)
            # frame.addfailure(failure, 1)
        elif error is not None:
            frame.error.increment(error, 1)
            # frame.adderror(error, 1)
        else:
            frame.success.increment(1)
            # frame.addsuccess(1)
            frame.client.add(self.clientid)

        # Set window's end time
        frame.end = timeline.end

        # # If oldframe was popped, add its values to current window
        # if key == curstart:
        #     window.update(oldframe)

    async def __call__(self, *args, **kwargs):
        """Measure coroutine runtime"""
        timeline = self.timeline

        # Default values
        ret = None
        failure = None
        error = None

        # Call the wrapped corofunc
        with self:
            try:
                ret = await self.corofunc(*args, **kwargs)
            except Failure as e:
                failure = str(e.args[0])
            except Exception as e:
                error = repr(e)
            finally:
                timeline.end = perf_counter()
                timeline.end_date = now()
            self.errors = ErrorMessage(error, failure)

        return ret


class LoadTimeline(defaultdict):
    """Store counts of wrapped coroutines that have completed"""

    def __init__(self):
        super().__init__(TimelineFrame)
        self.start = None
        self.start_date = None
        self.end = None
        self.end_date = None

    def __deepcopy__(self, memo):
        """Create a deepcopy of this LoadTimeline instance"""
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

    def mkwrapper(self, corofunc, *, name=None, clientid=None):
        """Create corofunc wrapper"""
        monitor = CoroMonitor(self, corofunc, name, clientid)

        @wraps(corofunc)
        async def wrapper(*args, **kwargs):
            return await monitor(*args, **kwargs)

        return wraps(corofunc)(monitor.__call__)

    def __call__(self, corofunc=None, *, name=None, clientid=None):
        """Decorator that records rate and response time of a corofunc"""
        if name is None:
            raise ValueError('name not given')
        elif not isinstance(name, str):
            msg = ('name expected str, got {} instead'.
                   format(type(name).__name__))
            raise TypeError(msg)

        deco = partial(self.mkwrapper, name=name, clientid=clientid)
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


measure = LoadTimeline()


# ============================================================================
# CountStore
# ============================================================================


# class Count:
#     __slots__ = ('frames', 'window_start', 'window_end', 'window_success',
#                  'window_error', 'window_failure', 'success', 'error',
#                  'failure', 'client')
#
#     def __init__(self):
#         self.frames = None
#         self.window_start = None
#         self.window_end = None
#         self.window_success = 0
#         self.window_error = defaultdict(lambda: 0)
#         self.window_failure = defaultdict(lambda: 0)
#
#         self.success = 0
#         self.error = defaultdict(lambda: 0)
#         self.failure = defaultdict(lambda: 0)
#         self.client = set()
#
#     def addsuccess(self, val=1):
#         """Increment the success count"""
#         self.success += val
#         self.window_success += val
#
#     def adderror(self, err, val=1):
#         """Add an error and increment its count"""
#         self.error[err] += val
#         self.window_error[err] += val
#
#     def addfailure(self, fail, val=1):
#         """Add a failure and increment its count"""
#         self.failure[fail] += val
#         self.window_failure[fail] += val
#
#     def addclient(self, clientid):
#         """Increment the client count"""
#         self.client.add(clientid)
#
#     def resetclient(self):
#         """Set client to a new empty set"""
#         self.client = set()
#         self.window_success = 0
#         self.window_error = defaultdict(lambda: 0)
#         self.window_failure = defaultdict(lambda: 0)
#         self.window_end = None
#         if self.frames:
#             key = next(iter(self.frames))
#             self.window_start = self.frames[key].window_start
#         else:
#             self.window_start = None
#
#     def update(self, old):
#         """Update values with values of old"""
#         # Update client ids
#         self.client.update(old.client)
#
#         # Update success
#         self.addsuccess(old.success)
#
#         # Dot optimizations
#         adderror = self.adderror
#         addfailure = self.addfailure
#
#         # Update errors
#         for errtxt, val in old.error.items():
#             adderror(errtxt, val)
#
#         # Update failures
#         for errtxt, val in old.failure.items():
#             addfailure(errtxt, val)
#
#     def sum(self):
#         """Sum of all counts
#
#         Does not include client counts.
#
#         """
#         return sum(chain([self.success], self.error.values(),
#                          self.failure.values()))
#
#
# class CountStore(defaultdict):
#     """Store counts of wrapped coroutines that have completed"""
#
#     def __init__(self):
#         super().__init__(Count)
#         self.start = None
#         self.start_date = None
#         self.end = None
#         self.end_date = None
#
#     def __deepcopy__(self, memo):
#         """Create a deepcopy of this CountStore instance"""
#         copy = self.__class__()
#
#         # Copy attributes
#         copy.start = deepcopy(self.start, memo)
#         copy.start_date = deepcopy(self.start_date, memo)
#         copy.end = deepcopy(self.end, memo)
#         copy.end_date = deepcopy(self.end_date, memo)
#
#         # Copy dict
#         for k, v in self.items():
#             copy[k] = deepcopy(v, memo)
#
#         return copy
#
#     def _getoldframe(self, curstart, window):
#         """Get the oldest frame"""
#         allframes = window.frames
#
#         # Get oldest frame key
#         key = next(iter(allframes))
#         cur_is_oldest = (key == curstart)
#
#         # Pop current frame
#         curframe = allframes.pop(curstart)
#
#         # Get oldest frame
#         oldframe = curframe if cur_is_oldest else allframes[key]
#         return key, oldframe
#
#     def mkwrapper(self, corofunc, *, name=None, clientid=None):
#         """Create corofunc wrapper"""
#
#         @wraps(corofunc)
#         async def wrapper(*args, **kwargs):
#             """Measure coroutine runtime"""
#             window = self[name]
#             start = self.start
#             curstart = perf_counter()
#
#             # Add the first frame
#             if window.frames is None:
#                 window.frames = OrderedDict()
#             window.frames[curstart] = f = Count()
#             f.window_start = curstart
#
#             # Set the window's start time
#             if window.window_start is None:
#                 window.window_start = curstart
#
#             # Set countstore's start time
#             if start is None:
#                 self.start_date = now()
#                 self.start = start = curstart
#
#             # Default values
#             ret = None
#             failure = None
#             error = None
#
#             # Call the wrapped corofunc
#             try:
#                 ret = await corofunc(*args, **kwargs)
#             except Failure as e:
#                 failure = str(e.args[0])
#             except Exception as e:
#                 error = repr(e)
#             finally:
#                 self.end = curend = perf_counter()
#                 self.end_date = now()
#
#             # Get oldest frame, this also removes the current frame
#             key, oldframe = self._getoldframe(curstart, window)
#
#             # Update oldest frame
#             if failure is not None:
#                 oldframe.addfailure(failure, 1)
#             elif error is not None:
#                 oldframe.adderror(error, 1)
#             else:
#                 oldframe.addsuccess(1)
#                 oldframe.client.add(clientid)
#
#             # Set window's end time
#             window.window_end = curend
#
#             # If oldframe was popped, add its values to current window
#             if key == curstart:
#                 window.update(oldframe)
#
#             return ret
#
#         return wrapper
#
#     def __call__(self, corofunc=None, *, name=None, clientid=None):
#         """Decorator that records rate and response time of a corofunc"""
#         if name is None:
#             raise ValueError('name not given')
#         elif not isinstance(name, str):
#             msg = ('name expected str, got {} instead'.
#                    format(type(name).__name__))
#             raise TypeError(msg)
#
#         deco = partial(self.mkwrapper, name=name, clientid=clientid)
#         if corofunc is None:
#             return deco
#
#         return deco(corofunc)
#
#     def allresetclient(self):
#         """Call resetclient() on every stored Count object"""
#         for v in self.values():
#             v.resetclient()
#
#     def reset(self):
#         """Reset all counts to starting values"""
#         self.start = None
#         self.start_date = None
#         self.end = None
#         self.end_date = None
#         self.clear()
#
#
# measure = CountStore()


class SendTimeData:

    stop = False

    def __init__(self, timeline, *, flushwait=None, channel=None):
        if not isinstance(flushwait, (type(None), Timedelta)):
            msg = 'flushwait expected pandas.Timedelta, got {} instead'
            raise TypeError(msg.format(type(flushwait).__name__))

        if not isinstance(channel, (type(None), DataChannel)):
            msg = 'channel expected DataChannel, got {} instead'
            raise TypeError(msg.format(type(channel).__name__))

        self._timeline = timeline
        self._flushwait = (2 if flushwait is None else
                           flushwait.total_seconds())
        self._channel = timedata if channel is None else channel
        self._start = None

    async def __call__(self):
        """Store timedata"""
        wait = self._flushwait
        timeline = self._timeline
        self._start = timeline.start
        sendfunc = self.send
        while True:
            await asyncio.sleep(wait)
            if self.stop:
                break
            await sendfunc(timeline)
            timeline.allresetclient()
            if self.stop:
                break

    async def send(self, timeline):
        """Send snapshot diff"""
        mkdata = self.mkdata
        # end_date = snapshot.end_date
        channel = self._channel
        reset = False
        if self._start is None:
            self._start = timeline.start
        elif timeline.start != self._start:
            reset = True
            self._start = timeline.start
        async for k, count in ageniter(snapshot.items()):
        end_date = now()
        snapshot = list(timeline.items())
        async for k, window in ageniter(snapshot):
            data = mkdata(curtime, end_date, k, window, reset=reset)
            await channel.send(data)

    def mkdata(self, curtime, end_date, key, window, *, reset=False):
        """Calculate rate and response time"""
        # timeline = window.timeline
        # if timeline:
        #     # Get oldest frame
        #     frame = timeline.oldest()

        #     # Update current window with data from oldest frame
        #     window.update(frame)

        # Calculate
        if window.frame.end:
            curtime = window.frame.end
        delta = curtime - window.frame.start
        success_diff = window.frame.success.value
        numclient = len(window.client)
        rate = (success_diff / delta) if delta > 0 else 0

        error = window.frame.error
        failure = window.frame.failure

        data = CountStoreData(name=key, end=end_date, delta=delta, rate=rate,
                              error=error, failure=failure, reset=reset,
                              clientcount=numclient)
        return data

    # async def countdiff(self, count, prevcount):
    #     """Return dictionary with count differences"""
    #     diff = {}
        async for k, v in ageniter(count.items()):
    #         diff[k] = v - prevcount[k] if k in prevcount else v
    #     return diff

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
        async for datatype in ageniter(self.values()):
            async for datalist in ageniter(datatype.values()):
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
        async for k in ageniter(genkey):
            async for datalist in ageniter(self[k].values()):
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
            async for k, c in ageniter(error.items()):
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
            async for k, c in ageniter(failure.items()):
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
                async for k in ageniter(statsdict):
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
