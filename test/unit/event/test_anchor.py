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
from collections import defaultdict

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
def test_schedule_tasks_hastasks(testloop, anchorcls):
    """Any tasks not yet waiting for the event are scheduled"""
    event = anchorcls()
    val = {1: None}

    @event
    async def one(result, **kwargs):
        """one"""
        val[1] = 'coro ran: one'

    async def run():
        event.set()

    event.start(loop=testloop)

    t = asyncio.ensure_future(run(), loop=testloop)
    testloop.run_until_complete(t)

    assert val == {1: 'coro ran: one'}


# ============================================================================
# Test anchortype == AnchorType.last
# ============================================================================


def test_runlast_runtasks_first(testloop):
    """Run all waiting tasks before the designated last coro"""
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

    async def run():
        """run"""
        event.set(v='{}')

    event.start(loop=testloop)
    f = asyncio.gather(
        asyncio.ensure_future(run(), loop=testloop),
        *asyncio.Task.all_tasks(loop=testloop),
        loop=testloop
    )
    testloop.run_until_complete(f)

    result = (set(val[:-1]), val[-1])
    assert result == expected


def test_runlast_notasks(testloop):
    """Run with only the last coro"""
    event = RunLast()
    val = []
    expected = ['one']

    @event(runlast=True)
    async def one(result):
        """one"""
        val.append(result.v.format('one'))

    async def run():
        """run"""
        event.set(v='{}')

    event.start(loop=testloop)
    t = asyncio.Task.all_tasks(loop=testloop)
    f = asyncio.gather(run(), *t, loop=testloop)
    testloop.run_until_complete(f)

    assert val == expected


# ============================================================================
# Test anchortype == AnchorType.first
# ============================================================================


def test_runfirst_runtasks_last(testloop):
    """Run all waiting tasks after the designated first coro"""
    asyncio.set_event_loop(testloop)
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

    async def run():
        """run"""
        event.set(v='{}')

    event.start(loop=testloop)
    f = asyncio.gather(run(), *asyncio.Task.all_tasks(loop=testloop),
                       loop=testloop)
    testloop.run_until_complete(f)

    result = (val[0], set(val[1:]))
    assert result == expected


def test_runfirst_notasks(testloop):
    """Run with only the first coro"""
    asyncio.set_event_loop(testloop)
    event = RunFirst()
    val = []
    expected = ['one']

    @event(runfirst=True)
    async def one(result):
        """one"""
        val.append(result.v.format('one'))

    async def run():
        event.set(v='{}')

    event.start(loop=testloop)
    f = asyncio.gather(run(), *asyncio.Task.all_tasks(loop=testloop),
                       loop=testloop)
    testloop.run_until_complete(f)

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
# Test exception handling in anchor func
# ============================================================================


# Originally, exceptions raised in the anchor func will not be caught by the
# exception handler. This is due to the task created by ensure_future being
# assigned to a variable that prevents the task from being deleted. The fix
# that the below tests is that anchor exceptions are now caught by the
# exception handler.

def test_anchorfunc_exception(testloop):
    """Exceptions raised in anchor functions are caught"""

    event = RunFirst()
    caught = None

    def goterr(loop, context):
        """Handle error"""
        nonlocal caught
        caught = context['exception']

    @event(runfirst=True)
    async def anchor(result):
        """Raise an exception"""
        raise Exception('ANCHOR')

    async def run():
        """run"""
        event.set()

    testloop.set_exception_handler(goterr)
    event.start(loop=testloop)
    t = asyncio.ensure_future(run())
    testloop.run_until_complete(t)

    assert caught is not None
    assert isinstance(caught, Exception)
    assert caught.args == ('ANCHOR', )


# ============================================================================
# Test reschedule
# ============================================================================


def test_reschedule(testloop):
    """Reschedule until reschedule option is False"""
    event = RunFirst()
    val = defaultdict(lambda: 0)

    @event
    async def one(result):
        """one"""
        val['one'] += 1 + result.val

    @event
    async def two(result):
        """two"""
        val['two'] += 2 + result.val

    @event(runfirst=True)
    async def first(result):
        """run first"""
        val['first'] += result.val
        if val['first'] == 5:
            event.option.reschedule = False

    async def run():
        """run"""
        event.set(val=1)

    expected = dict(first=5, one=10, two=15)

    event.start(reschedule=True)
    f = asyncio.gather(run(), *asyncio.Task.all_tasks())
    testloop.run_until_complete(f)

    assert val == expected


# ============================================================================
# Test schedule kwarg on __call__() and add()
# ============================================================================


@pytest.mark.parametrize('sched', [True, False])
def test_anchortype_noschedule(sched):
    """If an anchortype is given, value of schedule kwarg is ignored"""
    event = Anchor()

    @event(anchortype=AnchorType.first, schedule=sched)
    async def one(result):
        """one"""

    assert event.tasks == set([one])


def test_noschedule(testloop):
    """Not scheduled coro funcs are run before tasks are scheduled"""
    event = Anchor()
    val = []

    @event(anchortype=AnchorType.first)
    async def one(result):
        """one"""
        val.append(1)

    @event
    async def two(result):
        """two"""
        val.append(2)

    @event
    async def three(result):
        """three"""
        val.append(3)

    @event(schedule=False)
    async def answer(result):
        """answer"""
        val.append(42)

    async def run():
        """run"""
        event.set()

    event.start()
    f = asyncio.gather(run(), *asyncio.Task.all_tasks())
    testloop.run_until_complete(f)

    assert val
    assert val[:2] == [1, 42]
    assert set(val[2:]) == set([2, 3])


def test_noschedule_manualwrap(testloop):
    """Manual wrapping coro in noschedule event"""

    event = Anchor()
    val = []

    @event(anchortype=AnchorType.first)
    async def one(result):
        """one"""
        val.append(1)

    @event
    async def two(result):
        """two"""
        val.append(2)

    @event
    async def three(result):
        """three"""
        val.append(3)

    async def answer(result):
        """answer"""
        val.append(42)

    async def run():
        """run"""
        event.set()

    # Manually add answer coro as noschedule
    event(answer, schedule=False)

    event.start()
    f = asyncio.gather(run(), *asyncio.Task.all_tasks())
    testloop.run_until_complete(f)

    assert val
    assert val[:2] == [1, 42]
    assert set(val[2:]) == set([2, 3])


def test_noschedule_property():
    """noschedule property returns frozenset of all noschedule coro funcs"""
    event = Anchor()

    @event(anchortype=AnchorType.first)
    async def one(result):
        """one"""

    @event(schedule=False)
    async def two(result):
        """two"""

    @event
    async def three(result):
        """three"""

    @event(schedule=False)
    async def four(result):
        """four"""

    @event
    async def five(result):
        """five"""

    assert len(event.tasks) == 3
    assert len(event.noschedule) == 2

    assert event.tasks == set([one, three, five])
    assert event.noschedule == set([two, four])


# ============================================================================
#
# ============================================================================
