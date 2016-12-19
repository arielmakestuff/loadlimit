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
from collections import defaultdict, namedtuple
from functools import wraps
from itertools import chain, count
from pathlib import Path
from time import mktime, perf_counter

# Third-party imports
# import numpy as np
import pandas as pd
from pandas import (DataFrame, read_sql_table, Series, Timedelta)
from pandas.io import sql
from sqlalchemy import create_engine

# Local imports
from .channel import AnchorType, DataChannel
from .util import aiter, Namespace, now


# ============================================================================
# Globals
# ============================================================================


CountStoreData = namedtuple('CountStoreData',
                            ['name', 'end', 'delta', 'rate', 'error',
                             'failure'])


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
    __slots__ = ('success', 'error', 'failure')

    def __init__(self):
        self.success = 0
        self.error = defaultdict(lambda: 0)
        self.failure = defaultdict(lambda: 0)

    def addsuccess(self):
        """Increment the success count"""
        self.success += 1

    def adderror(self, err):
        """Add an error and increment its count"""
        self.error[err] += 1

    def addfailure(self, fail):
        """Add a failure and increment its count"""
        self.failure[fail] += 1

    def sum(self):
        """Sum of all counts"""
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

    def __call__(self, corofunc=None, *, name=None):
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
                if start is None:
                    self.start_date = now()
                    self.start = start = perf_counter()
                try:
                    await corofunc(*args, **kwargs)
                except Failure as e:
                    failure = str(e.args[0])
                    count.failure[failure] += 1
                except Exception as e:
                    count.error[repr(e)] += 1
                else:
                    count.success += 1
                finally:
                    self.end = perf_counter()
                    self.end_date = now()

            return wrapper

        if corofunc is None:
            return deco

        return deco(corofunc)


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

    async def __call__(self):
        """Store timedata"""
        wait = self._flushwait
        countstore = self._countstore
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
            await sendfunc(delta, snapshot, prevsnapshot)
            prevtime = curtime
            prevsnapshot = snapshot
            if self.stop:
                break

    async def send(self, delta, snapshot, prevsnapshot):
        """Send snapshot diff"""
        mkdata = self.mkdata
        end_date = self._countstore.end_date
        channel = self._channel
        async for k, count in aiter(snapshot.items()):
            prevcount = None if prevsnapshot is None else prevsnapshot[k]
            data = await mkdata(delta, end_date, k, count, prevcount)
            await channel.send(data)

    async def mkdata(self, delta, end_date, key, count, prevcount):
        """Calculate rate and response time"""
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
                              error=error, failure=failure)
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
    with (await statsdict.lock):
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

        response = (1 / rate) * 1000 if rate > 0 else 0
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
# Result classes
# ============================================================================


class Result:
    """Calculate result DataFrame from a Period"""

    def __init__(self, statsdict=None, countstore=None):
        self._statsdict = Period() if statsdict is None else statsdict
        self._countstore = measure if countstore is None else countstore
        self._vals = Namespace()

    def __iter__(self):
        for name, datatype in self._statsdict.items():
            yield (name, datatype['data'], datatype['error'],
                   datatype['failure'])

    def __enter__(self):
        """Start calculating the result"""
        countstore = self._countstore
        vals = self.vals
        vals.start = countstore.start_date
        vals.end = countstore.end_date
        return self

    def __exit__(self, errtype, err, errtb):
        """Finish calculating the result"""
        raise NotImplementedError

    def __call__(self):
        """Calculate the result"""
        calcfunc = self.calculate
        vals = self.vals
        with self:
            for name, data, error, failure in self:
                calcfunc(name, data, error, failure)

        return vals.results

    def calculate(self, name, data, error, failure):
        """Calculate results"""
        raise NotImplementedError

    def export(self, export_type, exportdir):
        """Export results"""
        raise NotImplementedError

    def exportdf(self, df, name, export_type, exportdir):
        """Export dataframe"""
        timestamp = int(mktime(now().timetuple()))
        filename = '{}_{}'.format(name, timestamp)
        exportdir = Path(exportdir)
        if export_type == 'csv':
            path = exportdir / '{}.{}'.format(filename, 'csv')
            df.to_csv(str(path), index_label=df.index.names)
        else:  # export_type == 'sqlite':
            path = str(exportdir / '{}.{}'.format(filename, 'db'))
            sqlengine = create_engine('sqlite:///{}'.format(path))
            with sqlengine.begin() as conn:
                df.to_sql('total', conn)

    @property
    def statsdict(self):
        """Return stored period statsdict"""
        return self._statsdict

    @property
    def countstore(self):
        """Return stored countstore"""
        return self._countstore

    @property
    def vals(self):
        """Return value namespace"""
        return self._vals


class Total(Result):
    """Calculate totals"""

    def __enter__(self):
        ret = super().__enter__()
        vals = self.vals

        # Duration (in seconds)
        vals.duration = (vals.end - vals.start).total_seconds()
        vals.results = {}

        vals.index = i = ['Total', 'Median', 'Average', 'Min', 'Max', 'Rate']
        vals.resultcls = namedtuple('ResultType', [n.lower() for n in i])
        vals.delta = None
        return ret

    def __exit__(self, errtype, err, errtb):
        """Finish calculations and save result"""
        vals = self.vals
        # countstore = self._countstore
        results = vals.results
        dfindex = (k for k, v in results.items() if v is not None)
        dfindex = list(sorted(dfindex, key=lambda k: k))
        # dfindex = list(sorted(results, key=lambda k: k))
        # data = [results[v] for v in dfindex if results[v] is not None]
        data = [results[v] for v in dfindex]
        if not data:
            vals.results = None
            return
        df = DataFrame(data, index=dfindex)
        total = df['Total'].sum()
        result = [total, df['Median'].median(), df['Average'].mean(),
                  df['Min'].min(), df['Max'].max(), total / vals.duration]
        result = DataFrame([Series(result, index=vals.index)],
                           index=['Totals'])
        vals.results = df = df.append(result)
        df.index.names = ['Name']

    def calculate(self, name, data, error, failure):
        """Calculate results"""
        vals = self.vals
        countstore = self.countstore

        # Number of iterations
        # This is a list of pandas.Series
        total = countstore[name].success
        numiter = len(data)
        if numiter == 0:
            vals.results[name] = None
            return

        # Create dataframe out of the timeseries and get only the response
        # field
        df = DataFrame(data, index=list(range(numiter)))
        delta = df['response']

        # Calculate stats
        r = [total, delta.median(), delta.mean(), delta.min(), delta.max(),
             total / vals.duration]
        r = vals.resultcls(*r)
        vals.results[name] = Series(r, index=vals.index)

    def export(self, export_type, exportdir):
        """Export total values"""
        results = self.vals.results
        if results is not None:
            self.exportdf(results, 'results', export_type, exportdir)


class TimeSeries(Result):
    """Calculate time series results"""

    def __enter__(self):
        ret = super().__enter__()
        vals = self.vals

        # Dates
        # start = vals.start
        # end = vals.end

        # date_array = np.linspace(start.value, end.value, vals.periods)
        # vals.daterange = pd.to_datetime(date_array)

        vals.response_result = {}
        vals.rate_result = {}
        return ret

    def __exit__(self, errtype, err, errtb):
        """Finish calculations and save result"""
        vals = self.vals
        response_result = vals.response_result
        rate_result = vals.rate_result
        df_response = None
        df_rate = None

        # Create response dataframe
        dfindex = (k for k, v in response_result.items() if v is not None)
        dfindex = list(sorted(dfindex, key=lambda k: k))
        data = [response_result[name] for name in dfindex]
        if data:
            # df_response = (DataFrame(data, index=dfindex).fillna(0) if data
            #                else None)
            df_response = DataFrame(data, index=dfindex)
            df_response.index.names = ['Name']

            # Create rate dataframe
            data = [rate_result[name] for name in dfindex]
            # df_rate = DataFrame(data, index=dfindex).fillna(0) if data else
            # None
            df_rate = DataFrame(data, index=dfindex)
            df_rate.index.names = ['Name']

        # Return both dataframes
        vals.results = (df_response, df_rate)
        for n in ['response_result', 'rate_result']:
            delattr(vals, n)

    def calculate(self, name, data, error, failure):
        """Calculate results"""
        vals = self.vals

        response = []
        rate = []

        # Number of iterations
        # total = countstore[name].success
        numiter = len(data)
        if numiter == 0:
            vals.response_result[name] = None
            vals.rate_result[name] = None
            return

        # Create dataframe out of the timeseries and get average response time
        # for each determined datetime period
        df = DataFrame(data, index=list(range(numiter)))
        daterange = df['end']
        response = df['response']
        response.index = daterange
        rate = df['rate']
        rate.index = daterange
        vals.response_result[name] = response  # Series
        vals.rate_result[name] = rate

    def export(self, export_type, exportdir):
        """Export total values"""
        df_response, df_rate = self.vals.results
        for name, df in zip(['response', 'rate'], [df_response, df_rate]):
            if df is not None:
                self.exportdf(df, name, export_type, exportdir)


class GeneralError(Result):
    """Calculate error results"""

    @property
    def errtype(self):
        """Return errtype string"""
        raise NotImplementedError

    def __enter__(self):
        ret = super().__enter__()
        vals = self.vals
        vals.results = {}
        return ret

    def __exit__(self, errtype, err, errtb):
        """Finish calculations and save result"""
        results = self.vals.results
        data = [v for v in results.values() if v is not None]
        self.vals.results = pd.concat(data) if data else None

    def calculate(self, name, *datatype):
        """Calculate results"""
        # data, error, failure = datatype
        vals = self.vals
        # countstore = self.countstore
        errtype, errind = self.errtype
        data = datatype[errind]

        # Number of iterations
        numiter = len(data)
        if numiter == 0:
            vals.results[name] = None
            return

        # Create dataframe out of the timeseries and get only the error field
        df = DataFrame(data, index=list(range(numiter)))
        df.insert(0, 'name', [name] * len(df))
        aggregate = {'count': 'sum'}
        result = df.groupby(['name', errtype]).agg(aggregate)
        result.columns = ['Total']
        result.index.names = ['Name', errtype.capitalize()]
        vals.results[name] = result

    def export(self, export_type, exportdir):
        """Export total values"""
        results = self.vals.results
        if results is not None:
            self.exportdf(results, self.errtype[0], export_type, exportdir)


class TotalError(GeneralError):
    """Calculate error results"""

    @property
    def errtype(self):
        """Return error errortype"""
        return 'error', 1


class TotalFailure(GeneralError):
    """Calculate failure results"""

    @property
    def errtype(self):
        """Return error errortype"""
        return 'failure', 2


# ============================================================================
# SQL versions of Results
# ============================================================================


class SQLResult:
    """Define iterating over values stored in an sql db"""

    def __init__(self, statsdict=None, countstore=None, sqltbl='period',
                 sqlengine=None):
        super().__init__(statsdict, countstore)
        vals = self.vals
        vals.sqltbl = sqltbl
        vals.sqlengine = sqlengine
        vals.datatype = ['timedata', 'error', 'failure']

    def __iter__(self):
        vals = self.vals
        sqlengine = vals.sqlengine
        with sqlengine.begin() as conn:
            for name in self._statsdict:
                # Generate table name
                curkey = sha1(name.encode('utf-8')).hexdigest()
                df = {}
                for k, substr in zip(vals.datatype,
                                     ['_', '_error_', '_failure_']):
                    tblname = '{}{}{}'.format(vals.sqltbl, substr, curkey)
                    df[k] = self.getdata(k, vals, sqlengine, conn, tblname)

                yield name, df['timedata'], df['error'], df['failure']

    def getdata(self, key, vals, sqlengine, sqlconn, tblname):
        """Get time data from db"""
        # Get number of rows in db
        hastable = sqlengine.dialect.has_table(sqlconn, tblname)
        if not hastable:
            return None
        df = read_sql_table(tblname, sqlconn, index_col='index',
                            parse_dates={'end': dict(utc=True)})
        # df['delta'] = (df['delta'].
        #                apply(partial(to_timedelta, unit='ns')))
        return df


class SQLTotal(SQLResult, Total):
    """Calculate totals from sql db"""

    def calculate(self, name, dfdata, dferror, dffailure):
        """Calculate results"""
        vals = self.vals
        countstore = self.countstore

        total = countstore[name].success
        numiter = len(dfdata.index)
        if numiter == 0:
            vals.results[name] = None
            return

        delta = dfdata['response']

        # Calculate stats
        r = [total, delta.median(), delta.mean(), delta.min(), delta.max(),
             total / vals.duration]
        r = vals.resultcls(*r)
        vals.results[name] = Series(r, index=vals.index)


class SQLTimeSeries(SQLResult, TimeSeries):
    """Calculate time series results from sql db"""

    def calculate(self, name, dfdata, dferror, dffailure):
        """Calculate results"""
        vals = self.vals

        # Number of iterations
        # total = countstore[name].success
        numiter = len(dfdata.index)
        if numiter == 0:
            vals.response_result[name] = None
            vals.rate_result[name] = None
            return

        response = []
        rate = []

        # Create dataframe out of the timeseries and get average response time
        # for each determined datetime period
        # df = DataFrame(data, index=list(range(numiter)))
        df = dfdata
        daterange = df['end']
        response = df['response']
        response.index = daterange
        rate = df['rate']
        rate.index = daterange
        vals.response_result[name] = response  # Series
        vals.rate_result[name] = rate


class SQLGeneralError:
    """Calculate error results"""

    def calculate(self, name, *datatype):
        """Calculate results"""
        vals = self.vals
        errtype, errind = self.errtype
        df = datatype[errind]
        if df is None:
            vals.results[name] = None
            return

        # Create dataframe out of the timeseries and get only the error field
        df.insert(0, 'name', [name] * len(df))
        aggregate = {'count': 'sum'}
        result = df.groupby(['name', errtype]).agg(aggregate)
        result.columns = ['Total']
        result.index.names = ['Name', errtype.capitalize()]
        vals.results[name] = result


class SQLTotalError(SQLResult, SQLGeneralError, TotalError):
    """Calculate total errors from sql db"""


class SQLTotalFailure(SQLResult, SQLGeneralError, TotalFailure):
    """Calculate total errors from sql db"""


# ============================================================================
#
# ============================================================================
