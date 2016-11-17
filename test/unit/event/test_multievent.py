# -*- coding: utf-8 -*-
# loadlimit/test/unit/event/test_multievent.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test MultiEvent class"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from collections import Counter

# Third-party imports
import pytest

# Local imports
from loadlimit.event import MultiEvent, LoadLimitEvent, RunLast


# ============================================================================
# Globals
# ============================================================================


pytestmark = pytest.mark.usefixtures('testlogging')


# ============================================================================
# Fixtures
# ============================================================================


@pytest.yield_fixture
def testloop(event_loop):
    """Cleanup event_loop run"""
    yield event_loop
    f = asyncio.gather(*asyncio.Task.all_tasks(loop=event_loop),
                       loop=event_loop)
    f.cancel()
    try:
        event_loop.run_until_complete(f)
    except asyncio.CancelledError:
        pass
    finally:
        event_loop.close()


@pytest.fixture
def multi():
    """docstring for multi"""
    event = MultiEvent()
    for i in range(5):
        event.__getitem__(i)
    return event


# ============================================================================
# Test using multiple events in a loop
# ============================================================================


def test_multiple_events(event_loop):
    """Setting one event does not affect other events in the MultiEvent"""

    multi = MultiEvent()
    val = []
    expected = [0, 1]

    @multi(eventid='one')
    async def runone(result):
        """runone"""
        await multi.wait('one')
        val.append(result.val)

    @multi(eventid='two')
    async def runtwo(result):
        """runtwo"""
        await multi.wait('two')
        val.append(result.val)

    async def runall():
        """runall"""
        val.append(0)
        multi.set('one', val=1)

    multi.start(loop=event_loop)
    f = asyncio.ensure_future(runall(), loop=event_loop)
    try:
        event_loop.run_until_complete(f)
        notdone = [t for t in asyncio.Task.all_tasks(loop=event_loop)
                   if not t.done()]
        f = asyncio.gather(*notdone, loop=event_loop)
        f.cancel()
        event_loop.run_until_complete(f)
    except asyncio.CancelledError:
        pass
    finally:
        event_loop.close()

    assert val == expected


# ============================================================================
# Test setting multiple events
# ============================================================================


def test_multiple_events_set(event_loop):
    """Calling set() without eventid sets all events"""

    multi = MultiEvent()
    val = set()
    expected = set([0, 1, 2])

    @multi(eventid='one')
    async def runone(result):
        """runone"""
        val.add(result.val)

    @multi(eventid='two')
    async def runtwo(result):
        """runtwo"""
        val.add(result.val+1)

    async def runall():
        """runall"""
        val.add(0)
        multi.set(val=1)

    multi.start(loop=event_loop)
    f = asyncio.ensure_future(runall(), loop=event_loop)
    try:
        event_loop.run_until_complete(f)
    finally:
        event_loop.close()

    assert val == expected


# ============================================================================
# Test adding a single coro to multiple events
# ============================================================================


@pytest.mark.parametrize('nocoro', [False, True])
def test_multiple_events_call(event_loop, nocoro):
    """Decorating corofuncs without eventid adds to all events"""
    multi = MultiEvent()
    for i in range(3):
        multi[i] = LoadLimitEvent()

    val = set()
    expected = set([1] * 3)
    deco = multi() if nocoro else multi

    @deco
    async def runone(result):
        """runone"""
        val.add(result.val)

    async def runall():
        """runall"""
        multi.set(val=1)

    multi.start(loop=event_loop)
    f = asyncio.ensure_future(runall(), loop=event_loop)
    try:
        event_loop.run_until_complete(f)
    finally:
        event_loop.close()

    assert val == expected


# ============================================================================
# Test iter
# ============================================================================


def test_iter():
    """Iterate over eventids"""
    multi = MultiEvent()
    names = set(['one', 'two', 'three'])
    for n in names:
        multi[n] = LoadLimitEvent()

    assert set(multi) == names


# ============================================================================
# Test __setitem__()
# ============================================================================


@pytest.mark.parametrize('val', [1, 42, 4.2, 'forty-two'])
def test_setitem_invalid_value(val):
    """Trying to set a non-LoadLimitEvent instance raises an error"""
    multi = MultiEvent()
    expected = ('val expected LoadLimitEvent, got {} instead'.
                format(type(val).__name__))
    with pytest.raises(TypeError) as err:
        multi[0] = val

    assert err.value.args == (expected, )


# ============================================================================
# Test __getitem__()
# ============================================================================


def test_getitem():
    """Retrieve stored eventid"""
    event = MultiEvent(RunLast)
    assert isinstance(event[0], RunLast)


# ============================================================================
# Test validate_factory()
# ============================================================================


def test_validate_factory_badval():
    """Raise TypeError if factory returns a non-LoadLimitEvent instance"""

    multi = MultiEvent(lambda: 42)
    expected = ('default_factory function returned int, '
                'expected LoadLimitEvent')
    with pytest.raises(TypeError) as err:
        multi.__getitem__(42)

    assert err.value.args == (expected, )


# ============================================================================
# Test keys()
# ============================================================================


def test_keys():
    """Iterate over eventids"""
    multi = MultiEvent()
    names = set(['one', 'two', 'three'])
    for n in names:
        multi[n] = LoadLimitEvent()

    assert set(multi.keys()) == names


# ============================================================================
# Test values()
# ============================================================================


def test_values():
    """Iterate over events"""
    multi = MultiEvent()
    e = LoadLimitEvent
    vals = {i: LoadLimitEvent() for i in range(5)}
    for n, e in vals.items():
        multi[n] = e

    expected = list(vals.values())
    assert all(e in expected for e in multi.values())


# ============================================================================
# Test items()
# ============================================================================


def test_items():
    """Iterate over (eventid, event) pairs"""
    multi = MultiEvent()
    e = LoadLimitEvent
    vals = {i: LoadLimitEvent() for i in range(5)}
    for n, e in vals.items():
        multi[n] = e

    assert dict(multi.items()) == vals


# ============================================================================
# Test clear()
# ============================================================================


def test_clear_noeventid(testloop):
    """Call clear() method of every stored event"""
    multi = MultiEvent()
    for i in range(5):
        multi.__getitem__(i)

    @multi
    async def runone(result):
        """runone"""

    multi.start(loop=testloop)
    assert all(event.started for event in multi.values())

    multi.set(v=42)
    multi.clear()

    assert all(not event.started for event in multi.values())


def test_clear_eventid(testloop):
    """Call clear() method of event with given eventid"""
    multi = MultiEvent()
    for i in range(5):
        multi.__getitem__(i)

    @multi
    async def runone(result):
        """runone"""

    multi.start(loop=testloop)
    assert all(event.started for event in multi.values())

    multi.set(v=42)
    multi.clear(1)

    assert all(event.started for i, event in multi.items()
               if i != 1)
    assert not multi[1].started

    # Cleanup
    for i, event in multi.items():
        if i != 1:
            event.clear()


# ============================================================================
# Test is_set()
# ============================================================================


@pytest.mark.parametrize('val', list(range(5)))
def test_is_set_noeventid(testloop, val):
    """Return dict of all stored event's is_set() value"""
    multi = MultiEvent()
    for i in range(5):
        multi.__getitem__(i)

    @multi
    async def one(result):
        """one"""

    multi.start(loop=testloop)
    assert multi.is_set() == {i: False for i in range(5)}
    multi.clear()

    multi(one)
    multi.start(loop=testloop)
    multi.set(val)
    expected = {i: (True if i == val else False) for i in range(5)}
    assert multi.is_set() == expected
    multi.clear(val)

    multi(one)
    multi.start(loop=testloop)
    multi.set()
    assert multi.is_set() == {i: True for i in range(5)}
    multi.clear()


def test_is_set_eventid(testloop):
    """Return result of given event's is_set() value"""
    multi = MultiEvent()
    for i in range(5):
        multi.__getitem__(i)

    @multi
    async def one(result):
        """one"""

    multi.start(loop=testloop)
    assert not multi.is_set(1)
    multi.set(1)
    assert multi.is_set(1)

    for i, event in multi.items():
        if i != 1:
            assert not event.is_set()
    multi.clear()


# ============================================================================
# Test wait()
# ============================================================================


def test_wait_noeventid(testloop):
    """Waits for all stored events' wait() method to complete"""
    multi = MultiEvent()
    val = []
    for i in range(5):
        multi.__getitem__(i)

    @multi
    async def one(result):
        """one"""
        val.append(result.val)

    async def waitforme():
        """waitforme"""
        await multi.wait()
        val.append(0)

    async def runall():
        """runall"""
        multi.set(val=1)

    t = [
        asyncio.ensure_future(runall(), loop=testloop),
        asyncio.ensure_future(waitforme(), loop=testloop)
    ]
    f = asyncio.gather(*t, loop=testloop)
    multi.start(loop=testloop)

    testloop.run_until_complete(f)

    expected = ([1] * 5) + [0]
    assert val == expected


# ============================================================================
# Test add()
# ============================================================================


def test_add_noeventid(testloop, multi):
    """Add tasks to all stored events"""
    val = []
    async def one(result):
        """one"""
        val.append(result.val + 1)

    async def two(result):
        """two"""
        val.append(result.val + 2)

    async def main():
        """main"""
        multi.set(val=0)

    multi.add(one, two)
    multi.start(loop=testloop)

    t = asyncio.ensure_future(main(), loop=testloop)
    f = asyncio.gather(t, loop=testloop)
    testloop.run_until_complete(f)

    c = Counter(val)
    assert len(val) == 10
    assert c[1] == 5
    assert c[2] == 5


def test_add_eventid(testloop, multi):
    """Add tasks to only the specified event"""
    val = []

    async def one(result):
        """one"""
        val.append(result.val + 1)

    async def two(result):
        """two"""
        val.append(result.val + 2)

    async def main():
        """main"""
        multi.set(eventid=0, val=0)

    multi.add(one, two, eventid=0)
    multi.start(eventid=0, loop=testloop)

    t = asyncio.ensure_future(main(), loop=testloop)
    f = asyncio.gather(t, loop=testloop)
    testloop.run_until_complete(f)

    c = Counter(val)
    assert len(val) == 2
    assert c[1] == 1
    assert c[2] == 1


# ============================================================================
#
# ============================================================================
