# -*- coding: utf-8 -*-
# test/unit/stat/test_flushtosql.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test flushtosql()"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from functools import partial

# Third-party imports
from pandas import DataFrame, read_sql_table, Series, Timestamp, to_timedelta
import pytest
from sqlalchemy import create_engine

# Local imports
import loadlimit.channel as channel
from loadlimit.core import BaseLoop
from loadlimit.result import SQLTotal
import loadlimit.stat as stat
from loadlimit.stat import CountStore, SendTimeData
from loadlimit.util import aiter, Namespace


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def fake_flushtosql(monkeypatch):
    """Setup fake flushtosql callable"""
    fake_flushtosql = stat.FlushToSQL()
    monkeypatch.setattr(stat, 'flushtosql', fake_flushtosql)


pytestmark = pytest.mark.usefixtures('fake_shutdown_channel',
                                     'fake_timedata_channel')


# ============================================================================
# Tests
# ============================================================================


@pytest.mark.parametrize('num', [10, 12])
def test_flushtosql(testloop, num):
    """updateperiod updates statsdict with timeseries data points

    num fixture allows testing the flushtosql_shutdown coro func for:

    * all data has already been flushed to the sql db
    * there's still some data remaining that needs to be flushed to sql db

    """
    measure = CountStore()

    # Setup sqlalchemy engine
    engine = create_engine('sqlite://')

    state = Namespace()
    timetotal = SQLTotal(state, sqlengine=engine, countstore=measure)

    # Create coro to time
    @measure(name='churn')
    async def churn(i):
        """Do nothing"""
        await asyncio.sleep(0)

    async def run():
        """run"""
        async for i in aiter(range(num)):
            await churn(i)
        await channel.shutdown.send(0)

    # Setup SendTimeData
    send = SendTimeData(measure, flushwait=to_timedelta(0, unit='s'),
                        channel=stat.timedata)

    # Add to shutdown channel
    channel.shutdown(send.shutdown)
    channel.shutdown(partial(stat.flushtosql_shutdown,
                             statsdict=timetotal.statsdict, sqlengine=engine))
    channel.shutdown(stat.timedata.shutdown)

    # Add flushtosql to timedata event
    stat.timedata(stat.flushtosql)

    # Run all the tasks
    with BaseLoop() as main:

        # Schedule SendTimeData coro
        asyncio.ensure_future(send())

        # Start every event, and ignore events that don't have any tasks
        stat.timedata.open()
        stat.timedata.start(asyncfunc=False, statsdict=timetotal.statsdict,
                            flushlimit=5, sqlengine=engine)
        asyncio.ensure_future(run())
        main.start()

    assert timetotal.statsdict.numdata == 0

    df = timetotal()

    assert isinstance(df, DataFrame)
    assert not df.empty


# ============================================================================
# Test FlushToSQL.flushdata()
# ============================================================================


@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
def test_flushdata_nodata(sqlengine, exctype):
    """Store error data in sqlite db"""
    statsdict = stat.Period()
    key = 'hello'
    namekey = 'world'
    sqltbl = 'period'
    tblname = '{}_{}'.format(sqltbl, namekey)
    end = Timestamp.now(tz='UTC')
    delta = to_timedelta(5, unit='s').total_seconds()
    err = exctype(42)
    data = Series([end, 1/5, delta, repr(err), 1],
                  index=['end', 'rate', 'response', 'error', 'count'])

    statsdict.adderror(key, data)

    # Send data to sqlite db
    with sqlengine.begin() as conn:
        stat.flushtosql.flushdata(statsdict, key, sqltbl, namekey, sqlengine,
                                  conn)

    # Check sqlite db
    with sqlengine.begin() as conn:
        assert not sqlengine.dialect.has_table(conn, tblname)


# ============================================================================
# Test FlushToSQL.flusherror()
# ============================================================================


@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
def test_flusherror(sqlengine, exctype):
    """Store error data in sqlite db"""
    statsdict = stat.Period()
    key = 'hello'
    namekey = 'world'
    sqltbl = 'period'
    tblname = '{}_error_{}'.format(sqltbl, namekey)
    end = Timestamp.now(tz='UTC')
    delta = to_timedelta(5, unit='s').total_seconds()
    err = exctype(42)
    data = Series([end, 1/5, delta, repr(err), 1],
                  index=['end', 'rate', 'response', 'error', 'count'])

    statsdict.adderror(key, data)

    # Send data to sqlite db
    with sqlengine.begin() as conn:
        stat.flushtosql.flusherror(statsdict, key, sqltbl, namekey, sqlengine,
                                   conn)

    # Check sqlite db
    with sqlengine.begin() as conn:
        assert sqlengine.dialect.has_table(conn, tblname)
        df = read_sql_table(tblname, conn, index_col='index',
                            parse_dates={'end': dict(utc=True)})
        assert len(df.index) == 1
        assert df.iloc[0].error == repr(err)


# ============================================================================
# Test FlushToSQL.flushfailure()
# ============================================================================


def test_flushfailure(sqlengine):
    """Store error data in sqlite db"""
    statsdict = stat.Period()
    key = 'hello'
    namekey = 'world'
    sqltbl = 'period'
    tblname = '{}_failure_{}'.format(sqltbl, namekey)
    end = Timestamp.now(tz='UTC')
    delta = to_timedelta(5, unit='s').total_seconds()
    err = stat.Failure(42)
    data = Series([end, 1/5, delta, str(err.args[0]), 1],
                  index=['end', 'rate', 'response', 'failure', 'count'])

    statsdict.addfailure(key, data)

    # Send data to sqlite db
    with sqlengine.begin() as conn:
        stat.flushtosql.flushfailure(statsdict, key, sqltbl, namekey,
                                     sqlengine, conn)

    # Check sqlite db
    with sqlengine.begin() as conn:
        assert sqlengine.dialect.has_table(conn, tblname)
        df = read_sql_table(tblname, conn, index_col='index',
                            parse_dates={'end': dict(utc=True)})
        assert len(df.index) == 1
        assert df.iloc[0].failure == str(err.args[0])


# ============================================================================
#
# ============================================================================
