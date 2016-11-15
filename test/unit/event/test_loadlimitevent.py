# -*- coding: utf-8 -*-
# loadlimit/test/unit/event/test_loadlimitevent.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test LoadLimitEvent class"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from asyncio import CancelledError

# Third-party imports
import pytest

# Local imports
from loadlimit.event import (EventNotStartedError, NoEventTasksError,
                             LoadLimitEvent)


# ============================================================================
# Globals
# ============================================================================


pytestmark = pytest.mark.usefixtures('testlogging')


# ============================================================================
# Test event_started()
# ============================================================================


@pytest.mark.parametrize('funcname', ['clear', 'is_set', 'set'])
def test_event_started_notstarted(funcname):
    """Error if clear called but event not started yet"""
    event = LoadLimitEvent()
    with pytest.raises(EventNotStartedError):
        getattr(event, funcname)()


# ============================================================================
# Test __iter__()
# ============================================================================


def test_iter(event_loop):
    """Iterates over all tasks waiting for the event"""

    event = LoadLimitEvent()

    @event
    async def one(result, **kwargs):
        """First event coro"""

    @event
    async def two(result, **kwargs):
        """Second event coro"""

    @event
    async def three(result, **kwargs):
        """Third event coro"""

    async def four():
        """Fourth event coro"""
        event.set()

    assert not event_loop.is_running()

    event.start(loop=event_loop)

    # Iterate over the event
    tasks = set(t for t in event)
    assert len(tasks) == 3
    assert tasks == event.waiting

    # Run all tasks and shutdown the loop
    t = asyncio.ensure_future(four(), loop=event_loop)
    f = asyncio.gather(t, *tasks, loop=event_loop)
    event_loop.run_until_complete(f)
    event_loop.close()


# ============================================================================
# Test clear()
# ============================================================================


def test_clear_canceltasks(event_loop):
    """clear() cancels all waiting tasks"""

    event = LoadLimitEvent()

    @event
    async def one(result, **kwargs):
        """First event coro"""

    async def two():
        """Second event coro"""
        tasks = event.waiting
        assert event.started
        assert len(tasks) == 1
        assert all(not t.done() for t in tasks)
        event.clear()
        await asyncio.gather(*tasks, loop=event_loop)
        assert not event.waiting
        assert not event.started
        assert all(t.cancelled() for t in tasks)

    assert not event_loop.is_running()
    event.start(loop=event_loop)

    # Run all tasks and shutdown the loop
    t = asyncio.ensure_future(two(), loop=event_loop)
    f = asyncio.gather(t, *event.waiting, loop=event_loop)
    try:
        event_loop.run_until_complete(f)
    except CancelledError:
        pass
    finally:
        event_loop.close()


# ============================================================================
# Test is_set()
# ============================================================================


def test_is_set(event_loop):
    """Return True if event has been set"""

    event = LoadLimitEvent()

    @event
    async def one(result, **kwargs):
        """First event coro"""
        assert event.is_set()

    async def two():
        """Set the event"""
        assert not event.is_set()
        event.set()

    event.start(loop=event_loop)

    # Run all tasks and shutdown the loop
    t = asyncio.ensure_future(two(), loop=event_loop)
    f = asyncio.gather(t, *event.waiting, loop=event_loop)
    event_loop.run_until_complete(f)
    event_loop.close()


# ============================================================================
# Test wait()
# ============================================================================


def test_wait_notstarted(event_loop):
    """Raises EventNotStartedError if event hasn't started"""
    event = LoadLimitEvent()
    assert not event.started
    coro = event.wait()
    with pytest.raises(EventNotStartedError):
        t = asyncio.ensure_future(coro, loop=event_loop)
        try:
            event_loop.run_until_complete(t)
        finally:
            event_loop.close()


# ============================================================================
# Test add()
# ============================================================================


@pytest.mark.parametrize('val', [42, lambda: None, max, LoadLimitEvent.add])
def test_add_notcoro(val):
    """Raise TypeError if adding an object that's not a coroutine"""
    event = LoadLimitEvent()

    expected = ('tasks expected coroutine, got {} instead'.
                format(type(val).__name__))
    with pytest.raises(TypeError) as err:
        event.add(val)

    assert err.value.args == (expected, )


# ============================================================================
# Test start()
# ============================================================================


def test_start_notasks(event_loop):
    """If there are no tasks, the event is not started"""
    event = LoadLimitEvent()
    assert not event.started
    assert len(event.tasks) == 0
    with pytest.raises(NoEventTasksError):
        event.start(loop=event_loop)
    assert not event.started


# ============================================================================
# Test _create_event()
# ============================================================================


def test_create_event_default_loop(event_loop):
    """Use current loop if loop arg is None"""
    event = LoadLimitEvent()

    @event
    async def one(result, **kwargs):
        """First event coro"""

    async def two():
        """Set the event"""
        event.set()

    asyncio.set_event_loop(event_loop)
    event.start()
    assert event._event._loop is event_loop

    # Run all tasks and shutdown the loop
    t = asyncio.ensure_future(two())
    f = asyncio.gather(t, *event.waiting)
    event_loop.run_until_complete(f)
    event_loop.close()


# ============================================================================
#
# ============================================================================