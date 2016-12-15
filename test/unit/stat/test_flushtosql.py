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
import loadlimit.stat as stat
from loadlimit.stat import SQLTotal, timecoro
from loadlimit.util import aiter


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
def test_flushtosql(num):
    """updateperiod updates statsdict with timeseries data points

    num fixture allows testing the flushtosql_shutdown coro func for:

    * all data has already been flushed to the sql db
    * there's still some data remaining that needs to be flushed to sql db

    """

    # Setup sqlalchemy engine
    engine = create_engine('sqlite://')

    timetotal = SQLTotal(sqlengine=engine)

    # Create coro to time
    @timecoro(name='churn')
    async def churn(i):
        """Do nothing"""
        print('CHURN')
        await asyncio.sleep(0)

    async def run():
        """run"""
        print('START RUN')
        async for i in aiter(range(num)):
            await churn(i)
        print('PRINT WAIT RECORDPERIOD')
        await stat.timedata.join()
        print('SHUTDOWN')
        await channel.shutdown.send(0)

    # Add to shutdown channel
    channel.shutdown(partial(stat.flushtosql_shutdown,
                             statsdict=timetotal.statsdict, sqlengine=engine))
    channel.shutdown(stat.timedata.shutdown)

    # Add flushtosql to timedata event
    stat.timedata(stat.flushtosql)

    # Run all the tasks
    with BaseLoop() as main:

        print('RECORD PERIOD SETUP')
        # Start every event, and ignore events that don't have any tasks
        stat.timedata.open()
        stat.timedata.start(asyncfunc=False, statsdict=timetotal.statsdict,
                            flushlimit=5, sqlengine=engine)
        print('SCHED RUN')
        asyncio.ensure_future(run())
        print('LOOP START')
        main.start()
        print('LOOP END')

    assert timetotal.statsdict.numdata == 0

    df = timetotal()

    assert isinstance(df, DataFrame)
    assert not df.empty


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
    delta = to_timedelta(5, unit='s')
    start = end - delta
    err = exctype(42)
    data = Series([start, end, delta, repr(err)],
                  index=['start', 'end', 'delta', 'error'])

    statsdict.adderror(key, data)

    # Send data to sqlite db
    with sqlengine.begin() as conn:
        stat.flushtosql.flusherror(statsdict, key, sqltbl, namekey, sqlengine,
                                   conn)

    # Check sqlite db
    with sqlengine.begin() as conn:
        assert sqlengine.dialect.has_table(conn, tblname)
        df = read_sql_table(tblname, conn, index_col='index',
                            parse_dates={'start': dict(utc=True),
                                         'end': dict(utc=True)})
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
    delta = to_timedelta(5, unit='s')
    start = end - delta
    err = stat.Failure(42)
    data = Series([start, end, delta, str(err.args[0])],
                  index=['start', 'end', 'delta', 'failure'])

    statsdict.addfailure(key, data)

    # Send data to sqlite db
    with sqlengine.begin() as conn:
        stat.flushtosql.flushfailure(statsdict, key, sqltbl, namekey,
                                     sqlengine, conn)

    # Check sqlite db
    with sqlengine.begin() as conn:
        assert sqlengine.dialect.has_table(conn, tblname)
        df = read_sql_table(tblname, conn, index_col='index',
                            parse_dates={'start': dict(utc=True),
                                         'end': dict(utc=True)})
        assert len(df.index) == 1
        assert df.iloc[0].failure == str(err.args[0])


# ============================================================================
#
# ============================================================================
