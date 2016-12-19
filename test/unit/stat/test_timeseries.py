# -*- coding: utf-8 -*-
# test/unit/stat/test_timeseries.py
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
from pandas import DataFrame, to_timedelta
import pytest
from sqlalchemy import create_engine

# Local imports
import loadlimit.channel as channel
from loadlimit.core import BaseLoop
import loadlimit.stat as stat
from loadlimit.stat import (CountStore, flushtosql, flushtosql_shutdown,
                            SendTimeData, SQLTimeSeries, TimeSeries)
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
    measure = CountStore()
    results = TimeSeries(countstore=measure)

    # Create coro to time
    @measure(name='churn')
    async def churn(i):
        """Do nothing"""
        await asyncio.sleep(0)

    async def run():
        """run"""
        async for i in aiter(range(500)):
            await churn(i)
        await channel.shutdown.send(0)

    # Setup SendTimeData
    send = SendTimeData(measure, flushwait=to_timedelta(0, unit='s'),
                        channel=stat.timedata)

    # Add to shutdown channel
    channel.shutdown(send.shutdown)
    channel.shutdown(stat.timedata.shutdown)

    # Run all the tasks
    with BaseLoop() as main:

        # Schedule SendTimeData coro
        asyncio.ensure_future(send())

        # Start every event, and ignore events that don't have any tasks
        stat.timedata.open()
        stat.timedata.start(asyncfunc=False, statsdict=results.statsdict)

        asyncio.ensure_future(run())
        main.start()

    ret = results()
    assert len(ret) == 2
    assert all(isinstance(r, DataFrame) and not r.empty for r in ret)

    # import pandas as pd
    # pd.set_option('display.max_columns', 500)
    # df_response, df_rate = ret
    # print(df_response)
    # print('-----------------------')
    # print(df_rate)


@pytest.mark.parametrize('num', [1000])
def test_sqltimeseries(testloop, num):
    """SQL timeseries works well"""
    measure = CountStore()

    # Setup sqlalchemy engine
    engine = create_engine('sqlite://')

    timeseries = SQLTimeSeries(sqlengine=engine, countstore=measure)

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

    # Add to shutdown event
    channel.shutdown(send.shutdown)
    channel.shutdown(partial(flushtosql_shutdown,
                             statsdict=timeseries.statsdict, sqlengine=engine))
    channel.shutdown(stat.timedata.shutdown)

    # Add flushtosql to timedata event
    stat.timedata(flushtosql)

    # Run all the tasks
    with BaseLoop() as main:

        # Schedule SendTimeData coro
        asyncio.ensure_future(send())

        # Start every event, and ignore events that don't have any tasks
        stat.timedata.open()
        stat.timedata.start(asyncfunc=False, statsdict=timeseries.statsdict,
                            flushlimit=500, sqlengine=engine)

        asyncio.ensure_future(run())
        main.start()

    assert timeseries.statsdict.numdata == 0

    results = timeseries()
    assert len(results) == 2
    assert all(isinstance(r, DataFrame) and not r.empty for r in results)

    # import pandas as pd
    # pd.set_option('display.max_columns', 500)
    # df_response, df_rate = results
    # print(df_response)
    # print('-----------------------')
    # print(df_rate)


# ============================================================================
# Test calculate (no data)
# ============================================================================


def test_calculate_nodata(statsdict):
    """Set results for a key to None if no data"""
    measure = CountStore()
    key = '42'
    calc = stat.TimeSeries(statsdict=statsdict, countstore=measure)
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
    measure = CountStore()
    calc = TimeSeries(statsdict=statsdict, countstore=measure)
    called = False

    def fake_exportdf(self, df, name, export_type, exportdir):
        nonlocal called
        called = True

    monkeypatch.setattr(TimeSeries, 'exportdf', fake_exportdf)

    with calc:
        pass
    results = calc.vals.results
    assert len(results) == 2
    assert results == (None, None)

    calc.export('EXPORTTYPE', 'EXPORTDIR')
    assert called is False


# ============================================================================
#
# ============================================================================
