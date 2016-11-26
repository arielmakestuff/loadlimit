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
# Third-party imports
from pandas import DataFrame

# Local imports
from loadlimit.core import BaseLoop
from loadlimit.event import NoEventTasksError, shutdown
from loadlimit.stat import recordperiod, timecoro, TimeSeries
from loadlimit.util import aiter


# ============================================================================
# Tests
# ============================================================================


def test_return_two_df():
    """Timeseries generates 2 dataframes"""
    results = TimeSeries()

    # Create coro to time
    @timecoro('churn')
    async def churn(i):
        """Do nothing"""
        await asyncio.sleep(0)

    async def run():
        """run"""
        async for i in aiter(range(500)):
            await churn(i)
        shutdown.set(exitcode=0)

    # Run all the tasks
    with BaseLoop() as main:

        # Start every event, and ignore events that don't have any tasks
        recordperiod.start(ignore=NoEventTasksError, reschedule=True,
                           statsdict=results.statsdict)

        asyncio.ensure_future(run())
        main.start()

    ret = results(periods=8)
    assert len(ret) == 2
    assert all(not r.empty and isinstance(r, DataFrame) for r in ret)


# ============================================================================
#
# ============================================================================
