# -*- coding: utf-8 -*-
# test/unit/util/test_event.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test Logger"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports

# Third-party imports
from pandas import Timestamp
import pytest

# Local imports
from loadlimit.util import Event, EventType, now


# ============================================================================
# Tests
# ============================================================================


@pytest.mark.parametrize('val', [42, 4.2, '42', [42], (42, )])
def test_init_event_type_badval(val):
    """Raise error if given a bad value for the event_type arg"""

    expected = ('event_type arg expected {} object, got {} object instead'.
                format(EventType.__name__, type(val).__name__))
    with pytest.raises(TypeError) as err:
        Event(val)

    assert err.value.args == (expected, )


@pytest.mark.parametrize('val', list(EventType))
def test_init_event_type_goodval(val):
    """Accept valid value for the event_type arg"""
    e = Event(val)
    assert e.type == val


@pytest.mark.parametrize('val', [42, 4.2, '42', [42], (42, )])
def test_init_timestamp_badval(val):
    """Raise error if given a bad value for the timestamp arg"""
    expected = ('timestamp arg expected {} object, got {} object instead'.
                format(Timestamp.__name__, type(val).__name__))
    with pytest.raises(TypeError) as err:
        Event(EventType.start, val)

    assert err.value.args == (expected, )


def test_init_timestamp_noval():
    """Automatically create the current timestamp if arg given None"""
    cur = now()
    e = Event(EventType.start)
    assert e.timestamp.floor('s') == cur.floor('s')


def test_init_timestamp_goodval():
    """Accept valid value for timestamp arg"""
    cur = now()
    e = Event(EventType.start, cur)
    assert e.timestamp == cur


@pytest.mark.parametrize('key', [0, 1])
def test_getitem_goodkey(key):
    """__getitem__() retrieves correct value"""
    e = Event(EventType.start, now())
    assert e[key] == e._val[key]


def test_getitem_badkey():
    """Raise error when given bad key"""
    expected = 'tuple index out of range'
    e = Event(EventType.start)
    with pytest.raises(IndexError) as err:
        e[42]

    assert err.value.args == (expected, )


def test_len():
    """Return number of items contained in the Event"""
    e = Event(EventType.start)
    assert len(e) == 2


# ============================================================================
#
# ============================================================================
