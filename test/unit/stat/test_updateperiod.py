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
from pandas import DataFrame, Series
import pytest

# Local imports
import loadlimit.channel as channel
from loadlimit.core import BaseLoop
import loadlimit.stat as stat
from loadlimit.stat import timecoro, Total
from loadlimit.util import aiter


# ============================================================================
# Fixtures
# ============================================================================


pytestmark = pytest.mark.usefixtures('fake_shutdown_channel',
                                     'fake_recordperiod_channel')


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
        await channel.shutdown.send(0)

    # Add to shutdown channel
    channel.shutdown(stat.recordperiod.shutdown)

    # Run all the tasks
    with BaseLoop() as main:

        # Start every event, and ignore events that don't have any tasks
        stat.recordperiod.open()
        stat.recordperiod.start(asyncfunc=False, statsdict=results.statsdict)

        asyncio.ensure_future(run())
        main.start()

    df = results()
    assert isinstance(df, DataFrame)
    assert not df.empty


# ============================================================================
# Test adding error
# ============================================================================


@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
def test_adderror(exctype, testloop):
    """updateperiod adds errors to statsdict"""
    statsdict = stat.Period()
    recordperiod = stat.recordperiod
    err = exctype(42)
    key = 'hello'

    @timecoro(name=key)
    async def coro():
        raise err

    async def runme():
        await coro()
        await recordperiod.join()

    recordperiod.open()
    recordperiod.start(statsdict=statsdict)
    testloop.run_until_complete(runme())

    assert statsdict.numerror(key) == 1

    stored_errors = list(statsdict.error(key))
    errordata = stored_errors[0]

    assert isinstance(errordata, Series)
    assert errordata.error == repr(err)


# ============================================================================
# Test adding failure
# ============================================================================


def test_addfailure(testloop):
    """updateperiod adds errors to statsdict"""
    statsdict = stat.Period()
    recordperiod = stat.recordperiod
    fail = stat.Failure(42)
    key = 'hello'

    @timecoro(name=key)
    async def coro():
        raise fail

    async def runme():
        await coro()
        await recordperiod.join()

    recordperiod.open()
    recordperiod.start(statsdict=statsdict)
    testloop.run_until_complete(runme())

    assert statsdict.numfailure(key) == 1

    stored_fail = list(statsdict.failure(key))
    faildata = stored_fail[0]

    assert isinstance(faildata, Series)
    assert faildata.failure == str(fail.args[0])


# ============================================================================
#
# ============================================================================
