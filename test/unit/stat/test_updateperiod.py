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

# Third-party imports
from pandas import DataFrame
import pytest

# Local imports
from loadlimit.core import BaseLoop
import loadlimit.event as event
from loadlimit.event import NoEventTasksError
import loadlimit.stat as stat
from loadlimit.stat import timecoro, Total
from loadlimit.util import aiter


# ============================================================================
# Fixtures
# ============================================================================


pytestmark = pytest.mark.usefixtures('fake_shutdown_event',
                                     'fake_recordperiod_event')


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

    # Create second coro to time
    @timecoro(name='churn_two')
    async def churn2(i):
        """Do nothing"""
        await asyncio.sleep(0)

    async def run():
        """run"""
        async for i in aiter(range(500)):
            await churn(i)
            await churn2(i)
        event.shutdown.set(exitcode=0)

    # Run all the tasks
    with BaseLoop() as main:

        # Start every event, and ignore events that don't have any tasks
        stat.recordperiod.start(ignore=NoEventTasksError, reschedule=True,
                                statsdict=results.statsdict)

        asyncio.ensure_future(run())
        main.start()

    df = results()
    assert isinstance(df, DataFrame)
    assert not df.empty


# ============================================================================
#
# ============================================================================
