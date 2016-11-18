# -*- coding: utf-8 -*-
# test/unit/event/test_runlast.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test RunLast class"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio

# Third-party imports
import pytest

# Local imports
from loadlimit.event import Anchor, AnchorType, RunLast, RunFirst


# ============================================================================
# Globals
# ============================================================================


pytestmark = pytest.mark.usefixtures('testlogging')


# ============================================================================
# Test _schedule_tasks()
# ============================================================================


@pytest.mark.parametrize('anchorcls', [RunLast, RunFirst])
def test_schedule_tasks_hastasks(event_loop, anchorcls):
    """Any tasks not yet waiting for the event are scheduled"""
    event = anchorcls()
    val = {1: None}

    @event
    async def one(result, **kwargs):
        """one"""
        val[1] = 'coro ran: one'

    event.start(loop=event_loop)
    event.set()

    f = asyncio.gather(*event.waiting, loop=event_loop)
    try:
        event_loop.run_until_complete(f)
    finally:
        event_loop.close()

    assert val == {1: 'coro ran: one'}


# ============================================================================
# Test anchortype == AnchorType.last
# ============================================================================


def test_runlast_runtasks_first(event_loop):
    """Run all waiting tasks before the designated last coro"""
    asyncio.set_event_loop(event_loop)
    event = RunLast()
    val = []
    expected = (set(['one', 'two', 'three']), 'four')

    @event
    async def one(result):
        """one"""
        val.append(result.v.format('one'))

    @event
    async def two(result):
        """one"""
        val.append(result.v.format('two'))

    @event
    async def three(result):
        """one"""
        val.append(result.v.format('three'))

    @event(runlast=True)
    async def four(result):
        """one"""
        val.append(result.v.format('four'))

    event.start(loop=event_loop)
    event.set(v='{}')
    try:
        f = asyncio.gather(*asyncio.Task.all_tasks(loop=event_loop),
                           loop=event_loop)
        event_loop.run_until_complete(f)
    finally:
        event_loop.close()

    result = (set(val[:-1]), val[-1])
    assert result == expected


def test_runlast_notasks(event_loop):
    """Run with only the last coro"""
    asyncio.set_event_loop(event_loop)
    event = RunLast()
    val = []
    expected = ['one']

    @event(runlast=True)
    async def one(result):
        """one"""
        val.append(result.v.format('one'))

    event.start(loop=event_loop)
    event.set(v='{}')
    try:
        f = asyncio.gather(*asyncio.Task.all_tasks(loop=event_loop),
                           loop=event_loop)
        event_loop.run_until_complete(f)
    finally:
        event_loop.close()

    assert val == expected


# ============================================================================
# Test anchortype == AnchorType.first
# ============================================================================


def test_runfirst_runtasks_last(event_loop):
    """Run all waiting tasks after the designated first coro"""
    asyncio.set_event_loop(event_loop)
    event = RunFirst()
    val = []
    expected = ('one', set(['two', 'three', 'four']))

    @event(runfirst=True)
    async def one(result):
        """one"""
        val.append(result.v.format('one'))

    @event
    async def two(result):
        """one"""
        val.append(result.v.format('two'))

    @event
    async def three(result):
        """one"""
        val.append(result.v.format('three'))

    @event
    async def four(result):
        """one"""
        val.append(result.v.format('four'))

    event.start(loop=event_loop)
    event.set(v='{}')
    try:
        f = asyncio.gather(*asyncio.Task.all_tasks(loop=event_loop),
                           loop=event_loop)
        event_loop.run_until_complete(f)
    finally:
        event_loop.close()

    result = (val[0], set(val[1:]))
    assert result == expected


def test_runfirst_notasks(event_loop):
    """Run with only the first coro"""
    asyncio.set_event_loop(event_loop)
    event = RunFirst()
    val = []
    expected = ['one']

    @event(runfirst=True)
    async def one(result):
        """one"""
        val.append(result.v.format('one'))

    event.start(loop=event_loop)
    event.set(v='{}')
    try:
        f = asyncio.gather(*asyncio.Task.all_tasks(loop=event_loop),
                           loop=event_loop)
        event_loop.run_until_complete(f)
    finally:
        event_loop.close()

    assert val == expected


# ============================================================================
# Test anchortype
# ============================================================================


@pytest.mark.parametrize('val', [
    v for v in [None] +
    list(AnchorType)
])
def test_anchortype_setvalid(val):
    """anchortype can only be set with AnchorType instance or None"""
    event = Anchor()
    event.anchortype = val
    assert event.anchortype == val


@pytest.mark.parametrize('val', ['hello', 42, 4.2, AnchorType])
def test_anchortype_setinvalid(val):
    """TypeError raised if anchortype given invalid value"""
    event = Anchor()
    expected = ('anchortype expected AnchorType or '
                'NoneType, got {} instead').format(type(val).__name__)

    with pytest.raises(TypeError) as err:
        event.anchortype = val

    assert err.value.args == (expected, )


# ============================================================================
# Test anchorfunc
# ============================================================================


def test_anchorfunc_none():
    """Return None if anchorfunc is not set"""
    event = Anchor()
    assert event.anchorfunc is None


def test_anchorfunc():
    """Return the set anchorfunc"""
    event = Anchor()

    async def noop(result):
        """noop"""

    event.anchortype = AnchorType.first
    event.add(noop)

    assert event.anchorfunc == noop


# ============================================================================
# Test anchortask
# ============================================================================


def test_anchortask_none():
    """Return None if no anchorfunc"""
    event = Anchor()
    assert event.anchortask is None


def test_anchortask_notsched():
    """Return None if anchorfunc not scheduled"""
    event = Anchor()

    @event(anchortype=AnchorType.first)
    async def noop(result):
        """noop"""

    assert event.anchorfunc == noop
    assert event.anchortask is None


def test_anchortask_sched(event_loop):
    """Return anchorfunc task if anchorfunc has been scheduled"""
    event = Anchor()

    @event(anchortype=AnchorType.first)
    async def noop(result):
        """noop"""
        assert event.anchortask is not None

    event.start(loop=event_loop)
    event.set()

    tasks = asyncio.Task.all_tasks(loop=event_loop)
    assert len(tasks) == 1

    f = asyncio.gather(*tasks, loop=event_loop)
    try:
        event_loop.run_until_complete(f)
    finally:
        event_loop.close()


# ============================================================================
#
# ============================================================================