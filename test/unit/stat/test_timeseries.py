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
from loadlimit.core import BaseLoop
import loadlimit.event as event
from loadlimit.event import NoEventTasksError
import loadlimit.stat as stat
from loadlimit.stat import (flushtosql, flushtosql_shutdown, SQLTimeSeries,
                            timecoro, TimeSeries)
from loadlimit.util import aiter


# ============================================================================
# Fixtures
# ============================================================================


pytestmark = pytest.mark.usefixtures('fake_shutdown_event',
                                     'fake_recordperiod_event')


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
        event.shutdown.set(exitcode=0)

    # Run all the tasks
    with BaseLoop() as main:

        # Start every event, and ignore events that don't have any tasks
        stat.recordperiod.start(ignore=NoEventTasksError, reschedule=True,
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
        event.shutdown.set(exitcode=0)

    # Add to shutdown event
    event.shutdown(partial(flushtosql_shutdown, statsdict=timedata.statsdict,
                           sqlengine=engine))

    # Add flushtosql to recordperiod event
    stat.recordperiod(flushtosql, schedule=False)

    # Run all the tasks
    with BaseLoop() as main:

        # Start every event, and ignore events that don't have any tasks
        stat.recordperiod.start(ignore=NoEventTasksError, reschedule=True,
                                statsdict=timedata.statsdict, flushlimit=500,
                                sqlengine=engine)

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
