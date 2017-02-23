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
from collections import (Counter, defaultdict, namedtuple, OrderedDict)
from functools import partial, wraps
from hashlib import sha1
from inspect import iscoroutinefunction
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


FrameData = namedtuple('FrameData',
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
# CoroMonitor
# ============================================================================


class CoroMonitor:
    __slots__ = ('timeline', 'corofunc', 'name', 'clientid', 'errors',
                 'curstart')

    def __init__(self, timeline, corofunc, name=None, clientid=None):
        if not isinstance(timeline, TimelineFrame):
            errmsg = ('timeline arg expected {} object, got {} object instead'.
                      format(TimelineFrame.__name__, type(timeline).__name__))
            raise TypeError(errmsg)
        if not iscoroutinefunction(corofunc):
            errmsg = ('corofunc arg expected coroutine function, '
                      'got {} object instead'.
                      format(type(corofunc).__name__))
            raise TypeError(errmsg)

        self.timeline = timeline
        self.corofunc = corofunc
        self.name = name
        self.clientid = clientid
        self.errors = ErrorMessage(None, None)
        self.curstart = None

    def __enter__(self):
        timeline = self.timeline
        self.curstart = curstart = perf_counter()

        # Add a new frame
        frame = timeline.newframe(start=curstart)
        timeline.addframe(frame)

        # Set the timeline frame's start time
        if timeline.frame.start is None:
            timeline.frame.start = curstart

        start_date = now()

        # Set timeline and timeline parent's start time
        for tl in [timeline, timeline.parent]:
            if tl.start is None:
                tl.start = curstart
                if hasattr(tl, 'start_date'):
                    tl.start_date = start_date

        if timeline.markphase:
            timeline.phasestart = curstart
        # if timeline.phasestart is None:
        #     timeline.phasestart = curstart

    def __exit__(self, exctype, exc, exctb):
        error, failure = self.errors
        timeline = self.timeline
        frame = timeline.frame
        curstart = self.curstart

        # Get oldest frame
        oldframe = timeline.oldest()

        # Remove current frame from window
        timeline.popframe(curstart)

        # Update oldest frame
        if failure is not None:
            oldframe.failure[failure] += 1
        elif error is not None:
            oldframe.error[error] += 1
        else:
            oldframe.success += 1
            oldframe.client.add(self.clientid)

        # Get drift
        # drift = curstart - (frame.start if timeline.phasestart is None
        #                     else timeline.phasestart)
        phasestart = timeline.phasestart
        drift = curstart - phasestart
        if drift < 0:
            drift = 0

        # Set window's end time
        # frame.end = (timeline.end if frame.end and curstart >= frame.end
        #              else timeline.end - drift)
        frame.end = timeline.end - drift
        #  print('WINDOW SIZE', phasestart, drift, frame.end - frame.start, timeline.end - curstart)

        # If oldframe was popped, add its values to current timeline
        if oldframe.start == curstart:
            timeline.update(oldframe)
            #  timeline.markphase = True
            if phasestart == curstart:
                timeline.markphase = True
                # timeline.phasestart = None

    async def __call__(self, args, kwargs):
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
                timeline.end = end = perf_counter()
                parent = timeline.parent
                parent.end = end
                parent.end_date = now()
                curframe = timeline.timeline[self.curstart]
                curframe.end = end
            self.errors = ErrorMessage(error, failure)

        return ret


# ============================================================================
# Frame
# ============================================================================


class Frame:
    __slots__ = ('start', 'end', 'client', 'success', 'error', 'failure')

    def __init__(self, start=None, end=None):
        self.start = start
        self.end = end
        self.reset()

    def __bool__(self):
        """True if contains any counts > 0"""
        chainiter = chain([self.success], e.values(), f.values())
        return any(v > 0 for v in chainiter)

    def sum(self):
        """Calculate the sum of all counters"""
        chainiter = chain([self.success], e.values(), f.values())
        return sum(chainiter)

    def reset(self):
        """Reset all counters"""
        self.client = set()
        self.success = 0
        self.error = Counter()
        self.failure = Counter()

    def update(self, frame):
        """Add counts from other frame"""
        if not isinstance(frame, Frame):
            errmsg = ('frame arg expected {} object, got {} object instead'.
                      format(Frame.__name__, type(frame).__name__))
            raise TypeError(errmsg)
        # Update client ids
        self.client.update(frame.client)

        # Update success
        self.success += frame.success

        # Dot optimizations
        error = self.error
        failure = self.failure

        # Update errors
        error.update(frame.error)

        # Update failures
        failure.update(frame.failure)


class TimelineFrame(Frame):
    __slots__ = ('timeline', 'frame', 'parent', 'phasestart', 'markphase')

    def __init__(self, parent):
        if not isinstance(parent, Timeline):
            errmsg = ('parent arg expected {} object, got {} object instead'.
                      format(Timeline.__name__, type(parent).__name__))
            raise TypeError(errmsg)
        self.timeline = OrderedDict()
        self.frame = self.newframe()
        self.parent = parent
        self.phasestart = None
        self.markphase = True
        super().__init__()

    @staticmethod
    def newframe(start=None, end=None):
        """Create a new frame"""
        return Frame(start=start, end=end)

    def addframe(self, frame):
        """Add a frame to the timeline"""
        if not isinstance(frame, Frame):
            errmsg = ('frame arg expected {} object, got {} object instead'.
                      format(Frame.__name__, type(frame).__name__))
            raise TypeError(errmsg)

        start = frame.start
        if start is None:
            frame.start = start = perf_counter()
        self.timeline[start] = frame

    def popframe(self, framestart):
        """Remove and return the frame that started at framestart"""
        # Pop current frame
        return self.timeline.pop(framestart)

    def resetframe(self):
        """Set timeline frame to new empty frames"""
        self.frame = curframe = self.newframe()
        if self.timeline:
            oldframe = self.oldest()
            curframe.start = curstart = oldframe.start
            self.phasestart = curstart
            self.markphase = False

    def update(self, frame):
        """Add counts from given frame"""
        super().update(frame)
        self.frame.update(frame)

    def oldest(self):
        """Get the oldest frame compared to frame started at framestart"""
        timeline = self.timeline
        if not timeline:
            return

        # Get oldest frame key
        key = next(iter(timeline))
        return timeline[key]


class Timeline:
    __slots__ = ('_timeline', 'start', 'end', 'start_date', 'end_date')

    def __init__(self):
        self._timeline = defaultdict(partial(TimelineFrame, self))
        self.start = None
        self.end = None
        self.start_date = None
        self.end_date = None

    def __getitem__(self, name):
        return self._timeline[name]

    def __iter__(self):
        return iter(self._timeline)

    def items(self):
        return self._timeline.items()

    def values(self):
        return self._timeline.values()

    def keys(self):
        return self._timeline.keys()

    def reset(self):
        """Set timeline frame to new empty frames"""
        for timeline in self._timeline.values():
            timeline.resetframe()

    def mkwrapper(self, corofunc, *, name=None, clientid=None):
        """Create corofunc wrapper"""

        @wraps(corofunc)
        async def wrapper(*args, **kwargs):
            timeline = self._timeline[name]
            monitor = CoroMonitor(timeline, corofunc, name, clientid)
            return await monitor(args, kwargs)

        return wrapper

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


measure = Timeline()


# ============================================================================
# SendTimeData
# ============================================================================


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
        self._cur_time = None
        self._last_time = None

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
            if self.stop:
                break

    async def send(self, timeline):
        """Send snapshot diff"""
        mkdata = self.mkdata
        channel = self._channel
        reset = False
        if self._start is None:
            self._start = timeline.start
        elif timeline.start != self._start:
            reset = True
            self._start = timeline.start
        self._last_time = self._cur_time
        self._cur_time = curtime = perf_counter()
        end_date = now()
        keys = frozenset(timeline)
        async for k in ageniter(keys):
            tlframe = timeline[k]
            data = mkdata(k, tlframe, curtime, end_date, reset=reset)
            await channel.send(data)

            # Reset timeline
            tlframe.resetframe()

    def mkdata(self, key, timeline, curtime, end_date, *, reset=False):
        """Calculate rate and response time"""
        snapshot = timeline.frame
        # Create new frame and copy counts of current window
        frame = timeline.newframe()
        frame.update(snapshot)
        frame.start = snapshot.start
        frame.end = snapshot.end
        #  extra_clients = 0
        if timeline.timeline:
            oldframe = timeline.oldest()
            frame.update(oldframe)
            #  extra_clients = len(oldframe.client)
            # if extra_clients:
            #     frame.update(oldframe)
            #     frame.end = curtime

        if frame.end:
            curtime = frame.end

        # Calculate raw delta
        # send_delta = (0 if self._last_time is None
        #               else self._cur_time - self._last_time)
        delta = 0 if frame.start is None else curtime - frame.start
        # print('FIRST DELTA', delta)
        # if delta > 0 and delta < send_delta:
        #     delta = send_delta
        # print('REAL DELTA', delta)

        # Calculate rate
        success_diff = frame.success
        #  numclient = len(frame.client) + extra_clients
        numclient = len(frame.client)
        rate = (success_diff / delta) if delta > 0 else 0
        #  print('STATS', success_diff)

        error = frame.error
        failure = frame.failure

        data = FrameData(name=key, end=end_date, delta=delta, rate=rate,
                         error=error, failure=failure, reset=reset,
                         clientcount=numclient)
        #  print(data)
        return data

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
            s = Series([end, clientrate, response],
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
