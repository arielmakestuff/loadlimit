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
from loadlimit.event import RunLast


# ============================================================================
# Globals
# ============================================================================


pytestmark = pytest.mark.usefixtures('testlogging')


# ============================================================================
# Test _schedule_tasks()
# ============================================================================


def test_schedule_tasks_hastasks(event_loop):
    """Any tasks not yet waiting for the event are scheduled"""
    event = RunLast()
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
# Test runlast()
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
#
# ============================================================================
