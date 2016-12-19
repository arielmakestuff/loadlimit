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
from pandas import DataFrame, Series, to_timedelta
import pytest

# Local imports
import loadlimit.channel as channel
from loadlimit.core import BaseLoop
import loadlimit.stat as stat
from loadlimit.stat import CountStore, Failure, SendTimeData, Total
from loadlimit.util import aiter


# ============================================================================
# Fixtures
# ============================================================================


pytestmark = pytest.mark.usefixtures('fake_shutdown_channel',
                                     'fake_timedata_channel')


# ============================================================================
# Tests
# ============================================================================


def test_updateperiod_coro(testloop):
    """updateperiod updates statsdict with timeseries data points"""

    measure = CountStore()
    results = Total(countstore=measure)

    # Create coro to time
    @measure(name='churn')
    async def churn(i):
        """Do nothing"""
        await asyncio.sleep(0)

    # Create second coro to time
    @measure(name='churn_two')
    async def churn2(i):
        """Do nothing"""
        await asyncio.sleep(0)

    async def run():
        """run"""
        async for i in aiter(range(500)):
            await churn(i)
            await churn2(i)
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

    df = results()
    assert isinstance(df, DataFrame)
    assert not df.empty


# ============================================================================
# Test adding error
# ============================================================================


@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
def test_adderror(exctype, testloop):
    """updateperiod adds errors to statsdict"""
    measure = CountStore()
    statsdict = stat.Period()
    timedata = stat.timedata
    err = exctype(42)
    key = 'hello'

    @measure(name=key)
    async def coro():
        raise err

    async def runme():
        await coro()
        await channel.shutdown.send(0)

    # Setup SendTimeData
    send = SendTimeData(measure, flushwait=to_timedelta(0, unit='s'),
                        channel=timedata)

    # Add to shutdown channel
    channel.shutdown(send.shutdown)
    channel.shutdown(timedata.shutdown)

    # Run all the tasks
    with BaseLoop() as main:

        # Schedule SendTimeData coro
        asyncio.ensure_future(send())

        # Start every event, and ignore events that don't have any tasks
        timedata.open()
        timedata.start(asyncfunc=False, statsdict=statsdict)

        asyncio.ensure_future(runme())
        main.start()

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
    measure = CountStore()
    statsdict = stat.Period()
    timedata = stat.timedata
    fail = Failure(42)
    key = 'hello'

    @measure(name=key)
    async def coro():
        raise fail

    async def runme():
        await coro()
        await channel.shutdown.send(0)

    # Setup SendTimeData
    send = SendTimeData(measure, flushwait=to_timedelta(0, unit='s'),
                        channel=timedata)

    # Add to shutdown channel
    channel.shutdown(send.shutdown)
    channel.shutdown(timedata.shutdown)

    # Run all the tasks
    with BaseLoop() as main:

        # Schedule SendTimeData coro
        asyncio.ensure_future(send())

        # Start every event, and ignore events that don't have any tasks
        timedata.open()
        timedata.start(asyncfunc=False, statsdict=statsdict)

        asyncio.ensure_future(runme())
        main.start()

    assert statsdict.numfailure(key) == 1

    stored_fail = list(statsdict.failure(key))
    faildata = stored_fail[0]

    assert isinstance(faildata, Series)
    assert faildata.failure == str(fail.args[0])


# ============================================================================
#
# ============================================================================
