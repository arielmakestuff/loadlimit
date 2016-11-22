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
# import uvloop

# Local imports
from loadlimit.core import BaseLoop
from loadlimit.event import NoEventTasksError, shutdown
from loadlimit.stat import Period, periodresults, recordperiod, timecoro
from loadlimit.util import aiter


# ============================================================================
# Tests
# ============================================================================


def test_updateperiod():
    """updateperiod updates statsdict with timeseries data points"""

    # Setup uvloop
    # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    stats = Period()

    # Create recordperiod anchor that updates stats
    # recordperiod(partial(updateperiod, stats), runfirst=True)

    # Create coro to time
    @timecoro('churn')
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
                           statsdict=stats)

        asyncio.ensure_future(run())
        main.start()

    df = periodresults(stats)
    assert len(df)


# ============================================================================
#
# ============================================================================
