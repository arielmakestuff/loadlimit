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
from hashlib import sha1
from collections import defaultdict, namedtuple
from functools import partial, wraps
from itertools import count
from pathlib import Path
from time import mktime, perf_counter

import logging

# Third-party imports
import numpy as np
import pandas as pd
from pandas import (DataFrame, read_sql_table, Series, Timestamp, to_timedelta)
from pandas.io import sql
from sqlalchemy import create_engine

# Local imports
from .channel import AnchorType, DataChannel
from .core import TimedTask
from .util import aiter, Namespace, now


# ============================================================================
# Errors
# ============================================================================


class Failure(Exception):
    """Exception returned by a coroutine to indicate failure"""


# ============================================================================
# Events
# ============================================================================


recordperiod = DataChannel(name='recordperiod')


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
# timecoro decorator
# ============================================================================


def timecoro(corofunc=None, *, name=None):
    """Records the start and stop time of the given corofunc"""
    if name is None:
        raise ValueError('name not given')
    elif not isinstance(name, str):
        msg = 'name expected str, got {} instead'.format(type(name).__name__)
        raise TypeError(msg)

    def deco(corofunc):
        """Function to decorate corofunc"""

        @wraps(corofunc)
        async def wrapper(*args, **kwargs):
            """Record start and stop time"""
            error = None
            failure = None
            times = TimedTask.count
            store = times[name]
            start = perf_counter()
            if store.start is None:
                store.start = start
            startdate = now()
            try:
                await corofunc(*args, **kwargs)
            except Failure as e:
                failure = str(e.args[0])
            except Exception as e:
                error = e
            finally:
                end = perf_counter()
                delta = end - start
                store.total += 1
                response = 1 / (store.total / (end - store.start))
                delta = to_timedelta(delta, unit='s')
                enddate = startdate + delta

            # Call recordperiod event with the start and end times
            data = Namespace(eventid=name, start=startdate, end=enddate,
                             error=error, failure=failure)
            await recordperiod.send(data)

        wrapper.__name__ = '_timecoro__{}'.format(wrapper.__name__)
        corofunc.wrapper = wrapper
        return wrapper

    if corofunc is None:
        return deco

    return deco(corofunc)


# ============================================================================
# Update stats coroutine
# ============================================================================


@recordperiod(anchortype=AnchorType.first)
async def updateperiod(data, *, statsdict=None, **kwargs):
    """Update a period/defaultdict(list) with period data point

    This is the anchor coro func for the recordperiod event.

    """
    name = data.eventid
    start, end = Timestamp(data.start), Timestamp(data.end)
    error = data.error
    failure = data.failure
    delta = end - start
    with (await statsdict.lock):
        if statsdict.start_date is None:
            statsdict.start_date = start
        statsdict.end_date = end

        # In-memory dict
        if error is not None:
            se = Series([start, end, delta, repr(error)],
                        index=['start', 'end', 'delta', 'error'])
            statsdict.adderror(name, se)
        elif failure is not None:
            sf = Series([start, end, delta, str(failure)],
                        index=['start', 'end', 'delta', 'failure'])
            statsdict.addfailure(name, sf)
        else:
            s = Series([start, end, delta], index=['start', 'end', 'delta'])
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
        timedelta64 = np.timedelta64
        slist = []
        for s in data:
            converted = list(s)
            val = [to_datetime(s[0].value),
                   to_datetime(s[1].value),
                   s[2] / timedelta64(1, 'ns')]
            val.extend(converted[3:])
            slist.append(val)
        return slist

    def flushdata(self, statsdict, key, sqltbl, namekey, sqlengine, sqlconn):
        """Flush time data"""
        curname = '{}_{}'.format(sqltbl, namekey)
        startind = self.countrows(sqlengine, sqlconn, curname)

        # Convert each series to use naive datetimes and nanosecond
        # int/float values instead of timedeltas
        slist = self.mktimeseries(statsdict.timedata(key))
        index = list(range(startind, startind + len(slist)))
        df = DataFrame(slist, index=index,
                       columns=['start', 'end', 'delta'])
        startind = startind + len(df.index)
        df.to_sql(curname, sqlconn, if_exists='append')

    def flusherror(self, statsdict, key, sqltbl, namekey, sqlengine, sqlconn):
        """Flush error data"""
        # Convert each exception into a string
        slist = [s[:3] + [str(s[3])] for s in
                 self.mktimeseries(statsdict.error(key))]
        if not slist:
            return
        curname = '{}_error_{}'.format(sqltbl, namekey)
        startind = self.countrows(sqlengine, sqlconn, curname)

        index = list(range(startind, startind + len(slist)))
        df = DataFrame(slist, index=index,
                       columns=['start', 'end', 'delta', 'error'])
        startind = startind + len(df.index)
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
                       columns=['start', 'end', 'delta', 'failure'])
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

    def __init__(self, statsdict=None):
        self._statsdict = Period() if statsdict is None else statsdict
        self._vals = Namespace()

    def __iter__(self):
        for name, datatype in self._statsdict.items():
            yield (name, datatype['data'], datatype['error'],
                   datatype['failure'])

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
            df.to_csv(str(path), index_label='Name')
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
        results = vals.results
        dfindex = list(v for v in sorted(results, key=lambda k: k)
                       if results[v] is not None)
        data = [results[v] for v in dfindex]
        if not data:
            vals.results = None
            return
        df = DataFrame(data, index=dfindex)
        delta = vals.delta
        # totseries = [df['Total'].sum(), delta.median(),
        #              delta.mean(), df['Min'].min(), df['Max'].max()]
        totseries = [df['Total'].sum(), delta.median(),
                     delta.mean(), delta.min(), delta.max()]
        totseries.append(totseries[0] / vals.duration)  # Add overall rate
        #  result = [totseries[0]] + totseries[3:]
        result = [totseries[0]] + totseries[-1:]
        result[1:1] = [v.total_seconds() * 1000 for v in totseries[1:-1]]
        result = DataFrame([Series(result, index=vals.index)],
                           index=['Totals'])
        vals.results = df = df.append(result)

    def calculate(self, name, data, error, failure):
        """Calculate results"""
        vals = self.vals

        # Number of iterations
        numiter = len(data)
        if numiter == 0:
            vals.results[name] = None
            return

        # Create dataframe out of the timeseries and get only the delta field
        df = DataFrame(data, index=list(range(numiter)))
        delta = df['delta']
        if vals.delta is None:
            vals.delta = delta
        else:
            vals.delta.append(delta, ignore_index=True)

        # Calculate stats
        r = [numiter]
        for val in [delta.median(), delta.mean(), delta.min(),
                    delta.max()]:
            r.append(val.total_seconds() * 1000)
        #  duration = delta.sum().total_seconds()
        duration = vals.duration
        r.append(0 if duration == 0 else numiter / duration)
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
        dfindex = list(n for n in sorted(response_result, key=lambda k: k)
                       if response_result[n] is not None)
        data = [response_result[name] for name in dfindex]
        df_response = (DataFrame(data, index=dfindex).fillna(0) if data
                       else None)

        # Create rate dataframe
        data = [rate_result[name] for name in dfindex]
        df_rate = DataFrame(data, index=dfindex).fillna(0) if data else None

        # Return both dataframes
        vals.results = (df_response, df_rate)
        for n in ['response_result', 'rate_result']:
            delattr(vals, n)

    def __call__(self, *, periods=1):
        self.vals.periods = periods
        return super().__call__()

    def calculate(self, name, data, error, failure):
        """Calculate results"""
        vals = self.vals

        response = []
        rate = []

        # Number of iterations
        numiter = len(data)
        if numiter == 0:
            vals.response_result[name] = None
            vals.rate_result[name] = None
            return

        # Create dataframe out of the timeseries and get average response time
        # for each determined datetime period
        df = DataFrame(data, index=list(range(numiter)))
        for d in vals.daterange:
            d = Timestamp(d, tz='UTC')
            delta = df.query('end <= @d')['delta']

            # Average response times
            avg_response = delta.mean().total_seconds() * 1000
            response.append(avg_response)

            # Iterations per second
            #  duration = delta.sum().total_seconds()
            duration = (d - vals.start).total_seconds()
            iter_per_sec = 0 if duration <= 0 else len(delta.index) / duration
            rate.append(iter_per_sec)

        daterange = vals.daterange
        vals.response_result[name] = Series(response, index=daterange)
        vals.rate_result[name] = Series(rate, index=daterange)

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
        aggregate = {
            errtype: 'count'
        }
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

    def __init__(self, statsdict=None, sqltbl='period', sqlengine=None):
        super().__init__(statsdict)
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
                            parse_dates={'start': dict(utc=True),
                                         'end': dict(utc=True)})
        df['delta'] = (df['delta'].
                       apply(partial(to_timedelta, unit='ns')))
        return df


class SQLTotal(SQLResult, Total):
    """Calculate totals from sql db"""

    def calculate(self, name, dfdata, dferror, dffailure):
        """Calculate results"""
        vals = self.vals

        df = dfdata['delta']
        numiter = len(dfdata)
        if numiter == 0:
            vals.results[name] = None
            return

        if vals.delta is None:
            vals.delta = df
        else:
            vals.delta.append(df, ignore_index=True)

        # Calculate stats
        r = [numiter]
        for val in [df.median(), df.mean(), df.min(), df.max()]:
            r.append(val.total_seconds() * 1000)
        duration = df.sum().total_seconds()
        r.append(0 if duration == 0 else numiter / duration)
        r = vals.resultcls(*r)
        vals.results[name] = Series(r, index=vals.index)


class SQLTimeSeries(SQLResult, TimeSeries):
    """Calculate time series results from sql db"""

    def calculate(self, name, dfdata, dferror, dffailure):
        """Calculate results"""
        vals = self.vals
        numiter = len(dfdata)
        if numiter == 0:
            vals.response_result[name] = None
            vals.rate_result[name] = None
            return

        response = []
        rate = []

        # Create dataframe out of the timeseries and get average response time
        # for each determined datetime period
        startpoint = vals.start
        for d in vals.daterange:
            d = Timestamp(d, tz='UTC')
            delta = dfdata.query('end <= @d')['delta']

            # Average response times
            avg_response = delta.mean().total_seconds() * 1000
            response.append(avg_response)

            # Iterations per second
            duration = (d - startpoint).total_seconds()
            iter_per_sec = 0 if duration <= 0 else len(delta) / duration
            rate.append(iter_per_sec)

        daterange = vals.daterange
        vals.response_result[name] = Series(response, index=daterange)
        vals.rate_result[name] = Series(rate, index=daterange)


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
        aggregate = {
            errtype: 'count'
        }
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
