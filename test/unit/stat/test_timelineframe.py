# -*- coding: utf-8 -*-
# test/unit/stat/test_timelineframe.py
# Copyright (C) 2017 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Tests for TimelineFrame"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from collections import Counter, defaultdict, OrderedDict

# Third-party imports
import pytest

# Local imports
from loadlimit.stat import Frame, TimelineFrame


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def frame():
    """Create a new frame"""
    return TimelineFrame()


# ============================================================================
# Test __init__
# ============================================================================


def test_init_attr_values(frame):
    """Set initial attr values"""
    # --------------------
    # Test
    # --------------------
    # TimelineFrame attrs
    assert isinstance(frame.names, set)
    assert not frame.names

    assert isinstance(frame.timeline, OrderedDict)
    assert not frame.timeline

    assert isinstance(frame.frame, Frame)
    assert frame.frame.start is None
    assert frame.frame.end is None
    assert not frame.frame.success
    assert not frame.frame.error
    assert not frame.frame.failure

    assert frame.start_date is None
    assert frame.end_date is None

    # Frame attrs
    # Start and end
    assert frame.start is None
    assert frame.end is None

    # Client set
    assert isinstance(frame.client, set)
    assert len(frame.client) == 0

    # Counters
    assert isinstance(frame.success, Counter)
    assert isinstance(frame.error, defaultdict)
    assert isinstance(frame.failure, defaultdict)
    assert not frame.success
    assert not frame.error
    assert not frame.failure

    assert frame.error.default_factory == Counter
    assert frame.failure.default_factory == Counter


# ============================================================================
# Test newframe
# ============================================================================


def test_newframe_noargs(frame):
    """Create a new frame without any args"""
    # --------------------
    # Test
    # --------------------
    new = frame.newframe()
    assert isinstance(new, Frame)
    assert new.start is None
    assert new.end is None
    assert not new.success
    assert not new.error
    assert not new.failure


@pytest.mark.parametrize('start,end', [
    (s, e) for s in [None, 1]
    for e in [None, 2]
])
def test_newframe_args(frame, start, end):
    """Create new Frame with given start and end args"""
    # --------------------
    # Test
    # --------------------
    new = frame.newframe(start=start, end=end)
    assert new.start == start
    assert new.end == end
    assert not new.success
    assert not new.error
    assert not new.failure


# ============================================================================
# Test addframe
# ============================================================================


@pytest.mark.parametrize('val', [42, 4.2, '42', (42, )])
def test_addframe_badval(frame, val):
    """Raise error if given a bad val"""
    # --------------------
    # Setup
    # --------------------
    expected = ('frame arg expected {} object, got {} object instead'.
                format(Frame.__name__, type(val).__name__))

    # --------------------
    # Test
    # --------------------
    with pytest.raises(TypeError) as err:
        frame.addframe(val)

    assert err.value.args == (expected, )


def test_addframe_empty_timeline(frame):
    """Add frame to an empty timeline"""
    # --------------------
    # Setup
    # --------------------
    new = frame.newframe(1)

    # --------------------
    # Test
    # --------------------
    frame.addframe(new)
    assert new.start == 1
    assert len(frame.timeline) == 1
    assert list(frame.timeline) == [1]
    assert list(frame.timeline.values()) == [new]


def test_addframe_nonempty_timeline(frame):
    """Add frame to timeline containing other frames"""
    # --------------------
    # Setup
    # --------------------
    old = frame.newframe(1)
    frame.timeline[1] = old
    new = frame.newframe(2)

    # --------------------
    # Test
    # --------------------
    frame.addframe(new)
    assert new.start == 2
    assert len(frame.timeline) == 2
    assert list(frame.timeline) == [old.start, new.start]
    assert list(frame.timeline.values()) == [old, new]


def test_addframe_setstart(frame):
    """Set frame's start value if it's None"""
    # --------------------
    # Setup
    # --------------------
    new = frame.newframe()

    # --------------------
    # Test
    # --------------------
    assert new.start is None
    frame.addframe(new)
    assert new.start is not None
    assert isinstance(new.start, float)
    assert new.start > 0
    assert list(frame.timeline) == [new.start]


# ============================================================================
# Test popframe
# ============================================================================


def test_popframe_with_start(frame):
    """Remove and return frame with given start value"""
    # --------------------
    # Setup
    # --------------------
    new = frame.newframe(42)
    frame.addframe(new)

    # --------------------
    # Test
    # --------------------
    popped = frame.popframe(42)
    assert not frame.timeline
    assert popped.start == 42
    assert popped is new


def test_popframe_nostart(frame):
    """Raise KeyError if given start value doesn't exist in timeline"""
    # --------------------
    # Test
    # --------------------
    with pytest.raises(KeyError) as err:
        frame.popframe(42)

    assert err.value.args == (42, )


# ============================================================================
# Test resetframe
# ============================================================================


def test_resetframe_newframe(frame):
    """Set timeline frame to a brand new frame object"""
    # --------------------
    # Setup
    # --------------------
    old = frame.frame
    old.start = 1
    old.end = 2
    old.success['a'] = 42
    old.error['a']['err'] = 43
    old.failure['a']['fail'] = 43

    # --------------------
    # Test
    # --------------------
    frame.resetframe()
    new = frame.frame
    assert new != old
    assert new.start is None
    assert new.end is None
    assert not new.success
    assert not new.error
    assert not new.failure


def test_resetframe_inherit_timeline_frame(frame):
    """Use oldest frame in timeline to set timeline frame's start value"""
    # --------------------
    # Setup
    # --------------------
    frame.addframe(Frame(42))
    frame.addframe(Frame(43))

    # --------------------
    # Test
    # --------------------
    frame.resetframe()
    assert frame.frame.start == 42


# ============================================================================
# Test update
# ============================================================================


def test_update_copy_counts(frame):
    """Add counts from a frame into both current frame and timeline frame"""
    # --------------------
    # Setup
    # --------------------
    name = 'hello'
    other = Frame(1, 2)
    frame.success[name] = 40
    frame.error[name]['err'] = 100

    other.success[name] = 2
    other.error[name]['err'] = 42
    other.failure[name]['fail'] = 1

    # --------------------
    # Test
    # --------------------
    frame.update(other)

    for f in [frame, frame.frame]:
        # Start and end times are not changed on either frame
        assert frame.start is None
        assert frame.end is None
        assert other.start == 1
        assert other.end == 2

        # Success is updated on frame only
        assert frame.success[name] == 42
        assert other.success[name] == 2

        # Error is updated on frame only
        assert frame.error[name]['err'] == 142
        assert other.error[name]['err'] == 42

        # Failure is updated on frame only
        assert frame.failure[name]['fail'] == 1
        assert other.failure[name]['fail'] == 1


# ============================================================================
# Test oldest
# ============================================================================


def test_oldest_notimeline(frame):
    """Do nothing if timeline is empty"""
    # --------------------
    # Test
    # --------------------
    assert frame.oldest() is None


def test_oldest_single_frame(frame):
    """Return the frame in timeline"""
    # --------------------
    # Setup
    # --------------------
    frame.addframe(Frame(42))

    # --------------------
    # Test
    # --------------------
    assert list(frame.timeline.values()) == [frame.oldest()]
    assert frame.oldest().start == 42


def test_oldest_multiframes(frame):
    """Return oldest frame in timeline"""
    # --------------------
    # Setup
    # --------------------
    for i in range(42, 44):
        frame.addframe(Frame(i))

    # --------------------
    # Test
    # --------------------
    assert frame.oldest().start == 42


# ============================================================================
# Test __call__
# ============================================================================


def test_call_noname(frame):
    """Raise exception if calling without a name"""
    # --------------------
    # Setup
    # --------------------
    expected = ('name not given')

    # --------------------
    # Test
    # --------------------
    with pytest.raises(ValueError) as err:
        frame()

    assert err.value.args == (expected, )


@pytest.mark.parametrize('val', [42, 4.2, (42, )])
def test_call_name_nonstr(frame, val):
    """Raise exception if name arg is given a non-str value"""
    # --------------------
    # Setup
    # --------------------
    expected = ('name expected str, got {} instead'.
                format(type(val).__name__))

    # --------------------
    # Test
    # --------------------
    with pytest.raises(TypeError) as err:
        frame(name=val)

    assert err.value.args == (expected, )


@pytest.mark.asyncio
async def test_call_decorate_corofunc_syntax(frame):
    """Decorate corofunc using decorator syntax sugar"""
    # --------------------
    # Setup
    # --------------------
    called = False
    name = 'test'

    # --------------------
    # Test
    # --------------------
    assert not frame.frame.success

    @frame(name=name)
    async def coro():
        nonlocal called
        called = True

    await coro()

    assert called
    for f in [frame.frame, frame]:
        assert f.success[name] == 1
        assert not f.error
        assert not f.failure


@pytest.mark.asyncio
async def test_call_decorate_corofunc_decorator(frame):
    """Decorate corofunc using decorator syntax sugar"""
    # --------------------
    # Setup
    # --------------------
    called = False
    name = 'test'

    # --------------------
    # Test
    # --------------------
    assert not frame.frame.success

    async def coro():
        nonlocal called
        called = True

    coro = frame(name=name)(coro)

    await coro()

    assert called
    for f in [frame.frame, frame]:
        assert f.success[name] == 1
        assert not f.error
        assert not f.failure


# ============================================================================
#
# ============================================================================
