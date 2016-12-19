# -*- coding: utf-8 -*-
# test/unit/stat/test_timeseris.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test timeseries()"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from functools import partial

# Third-party imports
from pandas import DataFrame
import pytest
from sqlalchemy import create_engine

# Local imports
import loadlimit.channel as channel
from loadlimit.core import BaseLoop
from loadlimit.event import NoEventTasksError
import loadlimit.stat as stat
from loadlimit.stat import (flushtosql, flushtosql_shutdown, SQLTimeSeries,
                            timecoro, TimeSeries)
from loadlimit.util import aiter


# ============================================================================
# Fixtures
# ============================================================================


pytestmark = pytest.mark.usefixtures('fake_shutdown_channel',
                                     'fake_timedata_channel')


# ============================================================================
# Tests
# ============================================================================


def test_return_two_df(testloop):
    """Timeseries generates 2 dataframes"""
    results = TimeSeries()

    # Create coro to time
    @timecoro(name='churn')
    async def churn(i):
        """Do nothing"""
        await asyncio.sleep(0)

    async def run():
        """run"""
        async for i in aiter(range(500)):
            await churn(i)
        await channel.shutdown.send(0)

    # Add to shutdown channel
    channel.shutdown(stat.timedata.shutdown)

    # Run all the tasks
    with BaseLoop() as main:

        # Start every event, and ignore events that don't have any tasks
        stat.timedata.open()
        stat.timedata.start(asyncfunc=False, statsdict=results.statsdict)

        asyncio.ensure_future(run())
        main.start()

    ret = results(periods=8)
    assert len(ret) == 2
    assert all(not r.empty and isinstance(r, DataFrame) for r in ret)


@pytest.mark.parametrize('num', [1000])
def test_sqltimeseries(testloop, num):
    """SQL timeseries works well"""

    # Setup sqlalchemy engine
    engine = create_engine('sqlite://')

    timeseries = SQLTimeSeries(sqlengine=engine)

    # Create coro to time
    @timecoro(name='churn')
    async def churn(i):
        """Do nothing"""
        await asyncio.sleep(0)

    async def run():
        """run"""
        async for i in aiter(range(num)):
            await churn(i)
        await channel.shutdown.send(0)

    # Add to shutdown event
    channel.shutdown(partial(flushtosql_shutdown,
                             statsdict=timeseries.statsdict, sqlengine=engine))
    channel.shutdown(stat.timedata.shutdown)

    # Add flushtosql to timedata event
    stat.timedata(flushtosql)

    # Run all the tasks
    with BaseLoop() as main:

        # Start every event, and ignore events that don't have any tasks
        stat.timedata.open()
        stat.timedata.start(asyncfunc=False, statsdict=timeseries.statsdict,
                            flushlimit=500, sqlengine=engine)

        asyncio.ensure_future(run())
        main.start()

    assert timeseries.statsdict.numdata == 0

    df_response, df_rate = timeseries(periods=8)

    for df in [df_response, df_rate]:
        assert isinstance(df, DataFrame)
        assert not df.empty


# ============================================================================
# Test calculate (no data)
# ============================================================================


def test_calculate_nodata(statsdict):
    """Set results for a key to None if no data"""
    key = '42'
    calc = stat.TimeSeries(statsdict=statsdict)
    calc.vals.periods = 3
    calc.__enter__()
    calc.calculate(key, [], [], [])

    vals = calc.vals
    assert vals
    assert vals.response_result[key] is None
    assert vals.rate_result[key] is None


# ============================================================================
# Test export
# ============================================================================


def test_export_nodata(monkeypatch, statsdict):
    """Do not call exportdf() if there are no results"""

    key = '42'
    calc = TimeSeries(statsdict=statsdict)
    calc.vals.periods = 3
    called = False

    def fake_exportdf(self, df, name, export_type, exportdir):
        nonlocal called
        called = True

    monkeypatch.setattr(TimeSeries, 'exportdf', fake_exportdf)

    with calc:
        pass
    assert calc.vals.results == (None, None)

    calc.export('EXPORTTYPE', 'EXPORTDIR')
    assert called is False


# ============================================================================
#
# ============================================================================
