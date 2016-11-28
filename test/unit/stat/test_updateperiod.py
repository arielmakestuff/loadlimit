# -*- coding: utf-8 -*-
# test/unit/stat/test_updateperiod.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test updateperiod"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
# from functools import partial

# Third-party imports
from pandas import DataFrame

# Local imports
from loadlimit.core import BaseLoop
from loadlimit.event import NoEventTasksError, shutdown
from loadlimit.stat import recordperiod, timecoro, Total
from loadlimit.util import aiter


# ============================================================================
# Tests
# ============================================================================


def test_updateperiod():
    """updateperiod updates statsdict with timeseries data points"""

    results = Total()

    # Create coro to time
    @timecoro(name='churn')
    async def churn(i):
        """Do nothing"""
        await asyncio.sleep(0)

    async def run():
        """run"""
        async for i in aiter(range(1000)):
            await churn(i)
        shutdown.set(exitcode=0)

    # Run all the tasks
    with BaseLoop() as main:

        # Start every event, and ignore events that don't have any tasks
        recordperiod.start(ignore=NoEventTasksError, reschedule=True,
                           statsdict=results.statsdict)

        asyncio.ensure_future(run())
        main.start()

    df = results()
    assert isinstance(df, DataFrame)
    assert not df.empty


# ============================================================================
#
# ============================================================================
