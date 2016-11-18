# -*- coding: utf-8 -*-
# test/unit/stat/test_timecoro.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test timecoro decorator"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from random import randint

# Third-party imports
import pytest

# Local imports
from loadlimit import stat
from loadlimit.stat import timecoro, Period
from loadlimit.event import NoEventTasksError, recordtime, recordperiod


# ============================================================================
# Test sends time data
# ============================================================================


@pytest.mark.parametrize('endval', [1])
def test_send_time_data(testloop, endval):
    """Sends time data via recordtime event"""
    stats = Period()
    corofunc = []

    def mkfunc(i):
        """mkfunc"""

        @timecoro(str(i))
        async def noop():
            """do nothing"""
            await asyncio.sleep(1)

        return noop

    # Make bunches of coroutines
    for i in range(endval):
        corofunc.append(mkfunc(i))

    # Create recordtime anchor that updates stats
    @recordtime(runfirst=True)
    async def update_stats(result):
        """docstring for update_stats"""
        stats[result.eventid].append(result.delta)

    # Start every event, and ignore events that don't have any tasks
    for event in [recordtime, recordperiod]:
        event.start(ignore=NoEventTasksError)

    # Run all the tasks
    t = [f() for f in corofunc]
    f = asyncio.gather(*t, loop=testloop)
    testloop.run_until_complete(f)

    for _, vals in stats.items():
        for v in vals:
            assert round(v, -3) == 1000


# ============================================================================
# Test sends period data
# ============================================================================


@pytest.mark.parametrize('endval', [1])
def test_send_period_data(testloop, endval):
    """Sends time data via recordperiod event"""
    stats = Period()
    corofunc = []

    def mkfunc(i):
        """mkfunc"""

        @timecoro(str(i))
        async def noop():
            """do nothing"""
            await asyncio.sleep(1)

        return noop

    # Make bunches of coroutines
    for i in range(endval):
        corofunc.append(mkfunc(i))

    # Create recordperiod anchor that updates stats
    @recordperiod(runfirst=True)
    async def update_stats(result):
        """docstring for update_stats"""
        stats[result.eventid].append((result.start, result.end))

    # Start every event, and ignore events that don't have any tasks
    for event in [recordtime, recordperiod]:
        event.start(ignore=NoEventTasksError)

    # Run all the tasks
    t = [f() for f in corofunc]
    f = asyncio.gather(*t, loop=testloop)
    testloop.run_until_complete(f)

    for _, vals in stats.items():
        for start, end in vals:
            ms = round((end - start).total_seconds() * 1000)
            assert round(ms, -3) == 1000


# ============================================================================
# Test percentile
# ============================================================================


def test_percentile(monkeypatch):
    """percentile returns percentile of all values stored in the given key"""

    args = dict(values=None, percentile=None)

    def fake_percentile(values, percentile):
        """Fake percentile"""
        args['values'] = list(values)
        args['percentile'] = percentile
        return 42

    monkeypatch.setattr(stat.np, 'percentile', fake_percentile)

    stats = Period()
    stats[0].extend(randint(1, 50000) for _ in range(100))

    # Check percentile got called with correct values
    assert stats.percentile(0, 42) == 42
    assert args['values'] == stats[0]
    assert args['percentile'] == 42


# ============================================================================
#
# ============================================================================
