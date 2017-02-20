# -*- coding: utf-8 -*-
# test/unit/stat/test_coromonitor.py
# Copyright (C) 2017 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Tests for CoroMonitor"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from contextlib import ExitStack

# Third-party imports
from pandas import Timestamp
import pytest

# Local imports
# import loadlimit.stat as stat
from loadlimit.stat import (CoroMonitor, ErrorMessage, Failure, Frame,
                            TimelineFrame)
from loadlimit.util import now


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def default_args():
    """Return brand new timeline and corofunc values"""
    timeline = TimelineFrame()

    async def coro():
        pass

    return timeline, coro


@pytest.fixture
def monitor(default_args):
    """Return a CoroMonitor object created from default_args"""
    timeline, coro = default_args
    return CoroMonitor(timeline, coro)


# ============================================================================
# Test __init__
# ============================================================================


def test_init_default_attr_values(default_args):
    """Set attribute values"""
    # --------------------
    # Setup
    # --------------------
    timeline, coro = default_args
    m = CoroMonitor(timeline, coro)

    # --------------------
    # Test
    # --------------------
    assert m.timeline is timeline
    assert m.corofunc is coro
    assert m.name is None
    assert m.clientid is None
    assert m.errors == ErrorMessage(None, None)
    assert m.curstart is None

    # Name is not added to timeline.names set
    assert not timeline.names


@pytest.mark.parametrize('val', [42, 4.2, '42', (42, )])
def test_init_bad_timeline(val):
    """Raise error if given a bad timeline value"""
    # --------------------
    # Setup
    # --------------------
    async def coro():
        pass

    expected = ('timeline arg expected {} object, got {} object instead'.
                format(TimelineFrame.__name__, type(val).__name__))

    # --------------------
    # Test
    # --------------------
    with pytest.raises(TypeError) as err:
        CoroMonitor(val, coro)

    assert err.value.args == (expected, )


@pytest.mark.parametrize('val', [42, 4.2, '42', (42, ), min])
def test_init_bad_corofunc(val):
    """Raise error if given a bad corofunc value"""
    # --------------------
    # Setup
    # --------------------
    timeline = TimelineFrame()
    expected = ('corofunc arg expected coroutine function, '
                'got {} object instead'.format(type(val).__name__))

    # --------------------
    # Test
    # --------------------
    with pytest.raises(TypeError) as err:
        CoroMonitor(timeline, val)

    assert err.value.args == (expected, )


@pytest.mark.parametrize('name', ['hello', 42, 4.2, (42, )])
def test_init_name_attr(default_args, name):
    """Set name attribute"""
    # --------------------
    # Setup
    # --------------------
    timeline, coro = default_args

    # --------------------
    # Test
    # --------------------
    m = CoroMonitor(timeline, coro, name=name)
    assert m.name == name
    assert timeline.names == set([name])


@pytest.mark.parametrize('clientid', ['hello', 42, 4.2, (42, )])
def test_init_clientid_attr(default_args, clientid):
    """Set clientid attr"""
    # --------------------
    # Setup
    # --------------------
    timeline, coro = default_args

    # --------------------
    # Test
    # --------------------
    m = CoroMonitor(timeline, coro, clientid=clientid)
    assert m.clientid == clientid


# ============================================================================
# Test __enter__
# ============================================================================


def test_enter_curstart_attr(default_args):
    """curstart attribute is set after entering context"""
    # --------------------
    # Setup
    # --------------------
    timeline, coro = default_args
    m = CoroMonitor(timeline, coro)

    # --------------------
    # Test
    # --------------------
    with m:
        assert isinstance(m.curstart, float)
        assert m.curstart > 0

        # Must be set before exiting context
        timeline.end = timeline.start + 9000


def test_enter_new_timeline_frame(monitor):
    """A new timeline frame is created"""
    # --------------------
    # Setup
    # --------------------
    monitor.name = 'hello'
    timeline = monitor.timeline

    # --------------------
    # Test
    # --------------------
    assert not timeline.timeline

    with monitor:
        # New frame was created
        assert timeline.timeline
        assert len(timeline.timeline) == 1
        frame = list(timeline.timeline.values())[0]
        assert isinstance(frame, Frame)

        # The new frame has a start time
        assert frame.start is not None
        assert frame.start == monitor.curstart

        # Must be set before exiting context
        timeline.end = timeline.start + 9000


def test_enter_timeline_start_date(monitor):
    """Timeline start time is set"""
    # --------------------
    # Setup
    # --------------------
    timeline = monitor.timeline

    # --------------------
    # Test
    # --------------------
    with monitor:
        # timeline.start is set
        assert timeline.start is not None
        assert timeline.start == monitor.curstart

        # start_date is set
        assert isinstance(timeline.start_date, Timestamp)
        dt = now()
        assert timeline.start_date.floor('D') == dt.floor('D')

        # Must be set before exiting context
        timeline.end = timeline.start + 9000


# ============================================================================
# Test __exit__
# ============================================================================


def test_exit_frame_update_success(monitor):
    """Frame is updated with an incremented success count"""
    # --------------------
    # Setup
    # --------------------
    name = 'hello'
    monitor.name = name
    timeline = monitor.timeline

    # --------------------
    # Test
    # --------------------
    with monitor:
        # Frame does not have a success count yet
        frame = list(timeline.timeline.values())[0]
        assert frame.success[name] == 0

        # Must be set before exiting context
        timeline.end = timeline.start + 9000

    # Frame was removed from timeline
    assert not timeline.timeline

    # Frame has incremented success value
    assert frame.success[name] == 1


def test_exit_frame_update_error(monitor):
    """Frame is updated with an incremented error count"""
    # --------------------
    # Setup
    # --------------------
    name = 'hello'
    errname = 'hello error'
    monitor.name = name
    timeline = monitor.timeline

    # Any failure message is ignored, cannot have error and failure messages at
    # the same time
    monitor.errors = ErrorMessage(errname, None)

    # --------------------
    # Test
    # --------------------
    with monitor:
        # Frame does not have an error count yet
        frame = list(timeline.timeline.values())[0]
        assert not frame.error

        # Must be set before exiting context
        timeline.end = timeline.start + 9000

    # Timeline frame has incremented error value
    assert timeline.frame.error[name][errname] == 1


def test_exit_frame_update_failure(monitor):
    """Frame is updated with an incremented failure count"""
    # --------------------
    # Setup
    # --------------------
    name = 'hello'
    failname = 'hello failure'
    monitor.name = name
    timeline = monitor.timeline
    monitor.errors = ErrorMessage('ignoreme', failname)

    # --------------------
    # Test
    # --------------------
    with monitor:
        # Frame does not have an error count yet
        frame = list(timeline.timeline.values())[0]
        assert not frame.failure

        # Must be set before exiting context
        timeline.end = timeline.start + 9000

    # Frame has incremented error value
    assert frame.failure[name][failname] == 1


def test_exit_copy_counts_to_timeline(monitor):
    """Copy counts to timeline if monitor corresponds to oldest frame"""
    # --------------------
    # Setup
    # --------------------
    name = 'hello'
    monitor.name = name
    timeline = monitor.timeline
    dnamelist = ['success', 'error', 'failure']

    # --------------------
    # Test
    # --------------------
    # Timeline and timeline frame both contain no counts
    for dname in dnamelist:
        assert not getattr(timeline.frame, dname)
        assert not getattr(timeline, dname)

    with monitor:
        frame = list(timeline.timeline.values())[0]
        frame.success[name] += 41
        frame.error[name]['err'] += 1
        frame.failure[name]['fail'] += 1

        # Must be set before exiting context
        timeline.end = timeline.start + 9000

    # Timeline and timeline frame have copied counts
    for dname in dnamelist:
        fcounts = getattr(frame, dname)
        tfcounts = getattr(timeline.frame, dname)
        tcounts = getattr(timeline, dname)

        assert tfcounts is not fcounts
        assert tcounts is not fcounts
        assert tfcounts == fcounts
        assert tcounts == fcounts


def test_exit_nocopy_to_timeline(default_args):
    """Frame counts are not copied to timeline if frame is not oldest"""
    # --------------------
    # Setup
    # --------------------
    timeline, coro = default_args
    m1 = CoroMonitor(timeline, coro, 'one')
    m2 = CoroMonitor(timeline, coro, 'one')

    # --------------------
    # Test
    # --------------------
    with ExitStack() as stack:
        stack.enter_context(m1)
        stack.enter_context(m2)

        # Increment most recent frame's count with special value
        # Note: oldest frame will get incremented twice, once for the inner m2
        # monitor, and again for the outer m1 monitor
        assert len(timeline.timeline) == 2
        frame = list(timeline.timeline.values())[-1]
        frame.success['one'] += 41

        # Must be set before exiting context
        timeline.end = timeline.start + 9000

    # Timeline and timeline frame only have copied counts from the first
    # monitor only
    assert timeline.success['one'] == 2
    assert timeline.frame.success['one'] == 2

    # There should be no frames left in the timeline
    assert not timeline.timeline


# ============================================================================
# Test __call__
# ============================================================================


@pytest.mark.asyncio
async def test_call_set_timeline_end(monitor):
    """timeline.end and timeline.end_date are set"""
    # --------------------
    # Setup
    # --------------------
    timeline = monitor.timeline

    # --------------------
    # Test
    # --------------------
    # Establish values before call
    assert timeline.end is None
    assert timeline.end_date is None

    await monitor((), {})

    # Establish values have changed
    assert timeline.end is not None
    assert timeline.end_date is not None

    # Check times
    assert isinstance(timeline.end, float)
    assert timeline.end > 0
    assert isinstance(timeline.end_date, Timestamp)
    assert timeline.end_date.floor('D') == now().floor('D')


@pytest.mark.asyncio
async def test_call_success_count(monitor):
    """Timeline success count incremented"""
    # --------------------
    # Setup
    # --------------------
    monitor.name = name = 'hello'
    timeline = monitor.timeline

    # --------------------
    # Test
    # --------------------
    # Establish values before call
    assert not timeline.frame.success
    assert not timeline.frame.error
    assert not timeline.frame.failure

    await monitor((), {})

    # Establish values have changed
    assert timeline.frame.success
    assert not timeline.frame.error
    assert not timeline.frame.failure

    # Check counts
    assert timeline.frame.success[name] == 1
    assert not timeline.frame.error
    assert not timeline.frame.failure


@pytest.mark.asyncio
async def test_call_error_count():
    """Timeline error count incremented"""
    # --------------------
    # Setup
    # --------------------
    name = 'hello'
    errname = '{} err'.format(name)
    err = RuntimeError(errname)

    async def coro():
        raise err

    timeline = TimelineFrame()
    monitor = CoroMonitor(timeline, coro, name)
    timeline = monitor.timeline

    # --------------------
    # Test
    # --------------------
    # Establish values before call
    assert not timeline.frame.success
    assert not timeline.frame.error
    assert not timeline.frame.failure

    await monitor((), {})

    # Establish values have changed
    assert not timeline.frame.success
    assert len(timeline.frame.error) == 1
    assert len(timeline.frame.error[name]) == 1
    assert not timeline.frame.failure

    # Check counts
    assert not timeline.frame.success
    assert timeline.frame.error[name][repr(err)] == 1
    assert not timeline.frame.failure


@pytest.mark.asyncio
async def test_call_failure_count():
    """Timeline failure count incremented"""
    # --------------------
    # Setup
    # --------------------
    name = 'hello'
    failname = '{} fail'.format(name)
    err = Failure(failname)

    async def coro():
        raise err

    timeline = TimelineFrame()
    monitor = CoroMonitor(timeline, coro, name)
    timeline = monitor.timeline

    # --------------------
    # Test
    # --------------------
    # Establish values before call
    assert not timeline.frame.success
    assert not timeline.frame.error
    assert not timeline.frame.failure

    await monitor((), {})

    # Establish values have changed
    assert not timeline.frame.success
    assert not timeline.frame.error
    assert len(timeline.frame.failure) == 1
    assert len(timeline.frame.failure[name]) == 1

    # Check counts
    assert not timeline.frame.success
    assert not timeline.frame.error
    assert timeline.frame.failure[name][failname] == 1


# ============================================================================
#
# ============================================================================
