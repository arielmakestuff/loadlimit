# -*- coding: utf-8 -*-
# test/unit/stat/test_timecoro.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test sleep_until coroutine"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from datetime import datetime, timedelta

# Third-party imports
import pytest

# Local imports
from loadlimit.coro import sleep_until, wait_until, NotUTCError
from loadlimit import coro
from loadlimit.util import now


# ============================================================================
# Test non-utc timezone
# ============================================================================


@pytest.mark.asyncio
async def test_nonutc():
    """Raise error if end.tzinfo is not timezone.utc"""
    end = datetime.now() + timedelta(days=1)
    with pytest.raises(NotUTCError):
        await sleep_until(end)


# ============================================================================
# Test no sleep
# ============================================================================


@pytest.mark.parametrize('end', [
    now(),
    now() - timedelta(seconds=1),
    now() - timedelta(days=200)
])
@pytest.mark.asyncio
async def test_no_sleep(end):
    """Don't sleep if end date 0 or negative seconds from now"""
    start = now()
    await sleep_until(end)
    finish = now()

    total = (finish - start).total_seconds()
    assert round(total) == 0


# ============================================================================
# Test sleep until time
# ============================================================================


@pytest.mark.asyncio
async def test_wait_until_datetime(monkeypatch, event_loop):
    """Waits until the end time"""

    async def fake_sleep(s):
        assert s == 5

    monkeypatch.setattr(coro, 'sleep', fake_sleep)

    start = now()
    endtime = start + timedelta(seconds=5)
    future = event_loop.create_future()
    await sleep_until(endtime, future, val='hello!')
    assert future.done()
    assert future.result() == dict(val='hello!')


# ============================================================================
# Test wait_until non-utc timezone
# ============================================================================


@pytest.mark.asyncio
async def test_wait_nonutc():
    """Raise error if end.tzinfo is not timezone.utc"""
    end = datetime.now() + timedelta(days=1)
    with pytest.raises(NotUTCError):
        await wait_until(end)


# ============================================================================
# Test wait_until no sleep
# ============================================================================


@pytest.mark.parametrize('end', [
    now(),
    now() - timedelta(seconds=1),
    now() - timedelta(days=200)
])
@pytest.mark.asyncio
async def test_no_wait(end):
    """Don't sleep if end date 0 or negative seconds from now"""
    start = now()
    await wait_until(end)
    finish = now()

    total = (finish - start).total_seconds()
    assert round(total) == 0


# ============================================================================
# Test wait
# ============================================================================


@pytest.mark.asyncio
async def test_wait_onehour(monkeypatch):
    """Wait one hour"""
    called_sleep = 0

    async def fake_sleep(s):
        nonlocal called_sleep
        called_sleep = called_sleep + 1
        assert s == 3600
        await asyncio.sleep(0)

    monkeypatch.setattr(coro, 'sleep', fake_sleep)

    onehour = now() + timedelta(seconds=3600)
    await wait_until(onehour)
    assert called_sleep == 1


@pytest.mark.asyncio
async def test_wait_2days(monkeypatch):
    """Wait 2 days"""
    called_sleep = 0
    days = 2 * (3600 * 24)
    start = now()
    endtime = start + timedelta(seconds=days)

    async def fake_sleep(s):
        nonlocal called_sleep
        called_sleep = called_sleep + 1
        if s:
            assert s == 3600
        else:
            assert s == 0
        await asyncio.sleep(0)

    def fake_now():
        if called_sleep == 5:
            return endtime
        return start

    monkeypatch.setattr(coro, 'sleep', fake_sleep)
    monkeypatch.setattr(coro, 'now', fake_now)

    await wait_until(endtime)
    assert called_sleep == 6


@pytest.mark.asyncio
async def test_wait_30min(monkeypatch):
    """Wait 30 minutes"""
    called_sleep = 0
    minutes = 30 * 60
    start = now()
    endtime = start + timedelta(seconds=minutes)

    async def fake_sleep(s):
        nonlocal called_sleep
        called_sleep = called_sleep + 1
        if s:
            assert s == minutes
        else:
            assert s == 0
        await asyncio.sleep(0)

    def fake_now():
        if called_sleep == 1:
            return endtime
        return start

    monkeypatch.setattr(coro, 'sleep', fake_sleep)
    monkeypatch.setattr(coro, 'now', fake_now)

    await wait_until(endtime)
    assert called_sleep == 1


# ============================================================================
# Test wait future
# ============================================================================


@pytest.mark.asyncio
async def test_wait_future(event_loop):
    """Future is set when wait_until wakes up"""
    future = event_loop.create_future()
    await wait_until(now(), future, val='hello')
    assert future.done()
    assert future.result() == dict(val='hello')


# ============================================================================
#
# ============================================================================
