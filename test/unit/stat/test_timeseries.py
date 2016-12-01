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
                                     'fake_recordperiod_channel')


# ============================================================================
# Tests
# ============================================================================


def test_return_two_df():
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
    channel.shutdown(stat.recordperiod.shutdown)

    # Run all the tasks
    with BaseLoop() as main:

        # Start every event, and ignore events that don't have any tasks
        stat.recordperiod.open()
        stat.recordperiod.start(asyncfunc=False,
                                statsdict=results.statsdict)

        asyncio.ensure_future(run())
        main.start()

    ret = results(periods=8)
    assert len(ret) == 2
    assert all(not r.empty and isinstance(r, DataFrame) for r in ret)


@pytest.mark.parametrize('num', [1000])
def test_sqltimeseries(num):
    """SQL timeseries works well"""

    # Setup sqlalchemy engine
    engine = create_engine('sqlite://')

    timedata = SQLTimeSeries(sqlengine=engine)

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
    channel.shutdown(partial(flushtosql_shutdown, statsdict=timedata.statsdict,
                             sqlengine=engine))
    channel.shutdown(stat.recordperiod.shutdown)

    # Add flushtosql to recordperiod event
    stat.recordperiod(flushtosql)

    # Run all the tasks
    with BaseLoop() as main:

        # Start every event, and ignore events that don't have any tasks
        stat.recordperiod.open()
        stat.recordperiod.start(asyncfunc=False, statsdict=timedata.statsdict,
                                flushlimit=500, sqlengine=engine)

        asyncio.ensure_future(run())
        main.start()

    assert timedata.statsdict.numdata == 0

    df_response, df_rate = timedata(periods=8)

    for df in [df_response, df_rate]:
        assert isinstance(df, DataFrame)
        assert not df.empty


# ============================================================================
#
# ============================================================================
