# -*- coding: utf-8 -*-
# loadlimit/result.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Result classes used to create results from various statistics"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from hashlib import sha1
from collections import namedtuple, OrderedDict
from pathlib import Path
from time import mktime

# Third-party imports
import pandas as pd
from pandas import (DataFrame, read_sql_table, Series)
from sqlalchemy import create_engine

# Local imports
from .stat import measure, Period
from .util import Namespace, now


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
        results = vals.results
        dfindex = (k for k, v in results.items() if v is not None)
        dfindex = list(sorted(dfindex, key=lambda k: k))
        data = [results[v] for v in dfindex]
        if not data:
            vals.results = None
            return
        df = DataFrame(data, index=dfindex)
        total = df['Total'].sum()
        rate = 0 if vals.duration == 0 else total / vals.duration
        result = [total, df['Median'].median(), df['Average'].mean(),
                  df['Min'].min(), df['Max'].max(), rate]
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
        rate = 0 if vals.duration == 0 else total / vals.duration
        r = [total, delta.median(), delta.mean(), delta.min(), delta.max(),
             rate]
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
        data = OrderedDict()
        for name in dfindex:
            data[name] = response_result[name]
        if data:
            df_response = DataFrame(data)
            df_response.index.names = ['Timestamp']

            # Create rate dataframe
            data = OrderedDict()
            for name in dfindex:
                data[name] = rate_result[name]
            df_rate = DataFrame(data)
            df_rate.index.names = ['Timestamp']

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
        rate = 0 if vals.duration == 0 else total / vals.duration
        r = [total, delta.median(), delta.mean(), delta.min(), delta.max(),
             rate]
        r = vals.resultcls(*r)
        vals.results[name] = Series(r, index=vals.index)


class SQLTimeSeries(SQLResult, TimeSeries):
    """Calculate time series results from sql db"""

    def calculate(self, name, dfdata, dferror, dffailure):
        """Calculate results"""
        vals = self.vals

        # Number of iterations
        numiter = len(dfdata.index)
        if numiter == 0:
            vals.response_result[name] = None
            vals.rate_result[name] = None
            return

        response = []
        rate = []

        # Create dataframe out of the timeseries and get average response time
        # for each determined datetime period
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
