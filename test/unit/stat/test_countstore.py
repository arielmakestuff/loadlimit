# -*- coding: utf-8 -*-
# test/unit/stat/test_countstore.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test Count and CountStore classes"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from asyncio import iscoroutinefunction
from collections import defaultdict

# Third-party imports
import pytest

# Local imports
import loadlimit.stat as stat
from loadlimit.stat import Count, CountStore
from loadlimit.util import now


# ============================================================================
# Test Count
# ============================================================================


def test_count_init():
    """Initialize Count objects with attrs set to 0"""
    c = Count()
    assert c.success == 0
    for attr in ['error', 'failure']:
        assert isinstance(getattr(c, attr), defaultdict)


def test_count_newerror():
    """Adding a new error initializes its count to 0"""
    c = Count()
    assert c.error[42] == 0


def test_count_newfailure():
    """Adding a new failure initializes its count to 0"""
    c = Count()
    assert c.failure[42] == 0


def test_count_addsuccess():
    """Adding a success increments the success count"""
    c = Count()
    for i in range(10):
        c.addsuccess()
        assert c.success == i + 1


def test_count_adderror():
    """Adding an error increments the error count"""
    c = Count()
    key = 42
    for i in range(10):
        c.adderror(key)
        assert c.error[key] == i + 1


def test_count_addfailure():
    """Adding a failure increments the failure count"""
    c = Count()
    key = 42
    for i in range(10):
        c.addfailure(key)
        assert c.failure[key] == i + 1


def test_count_sum_zero():
    """Return 0"""
    c = Count()
    assert c.sum() == 0


@pytest.mark.parametrize('success,error,failure', [
    (s, e, f) for s in [True, False]
    for e in [True, False]
    for f in [True, False]
])
def test_count_sum(success, error, failure):
    """Returns the correct sum"""
    c = Count()
    expected = 0
    if success:
        c.addsuccess()
        expected += 1
    if error:
        c.adderror('error')
        expected += 1
    if c.failure:
        c.addfailure('failure')
        expected += 1

    assert c.sum() == expected


# ============================================================================
# Test CountStore.__init__
# ============================================================================


def test_countstore_init_default():
    """New keys are automatically initialized with a Count object"""
    c = CountStore()
    assert not c
    assert isinstance(c[42], Count)
    assert c
    assert len(c) == 1


# ============================================================================
# Test CountStore.__call__
# ============================================================================


def test_countstore_call_noname():
    """Raise error if not called with a name"""

    c = CountStore()
    expected = 'name not given'
    with pytest.raises(ValueError) as err:
        c()

    assert err.value.args == (expected, )


@pytest.mark.parametrize('val', [42, 4.2, [42], {}, ()])
def test_countstore_call_name_notstr(val):
    """Raise error if given a non-str name"""
    expected = 'name expected str, got {} instead'.format(type(val).__name__)
    c = CountStore()
    with pytest.raises(TypeError) as err:
        c(name=val)

    assert err.value.args == (expected, )


def test_countstore_call_decorator():
    """Calling immediately with corofunc"""

    measure = CountStore()

    async def one():
        """one"""

    wrapped = measure(one, name='one')

    assert wrapped is not one
    assert iscoroutinefunction(wrapped)
    assert hasattr(wrapped, '__wrapped__')
    assert wrapped.__wrapped__ is one


# ============================================================================
# Test CountStore.__call__ measurement
# ============================================================================


@pytest.mark.asyncio
async def test_countstore_measure_setkey(monkeypatch):
    """Adds name as a CountStore key"""
    measure = CountStore()
    called = False

    @measure(name='run')
    async def noop():
        """Do nothing"""
        nonlocal called
        called = True

    assert not measure

    await noop()

    assert len(measure) == 1
    assert list(measure.keys()) == ['run']
    assert called is True
    assert measure['run'].sum() == 1
    assert measure['run'].success == 1


@pytest.mark.asyncio
async def test_countstore_measure_initstart(monkeypatch):
    """CountStore.start is set with a float"""

    measure = CountStore()
    called = False

    @measure(name='run')
    async def noop():
        """Do nothing"""
        nonlocal called
        called = True

    assert not measure

    await noop()

    assert measure.start is not None
    assert isinstance(measure.start, float)
    assert measure.start > 0

    start_date = measure.start_date
    cur = now()
    assert start_date is not None
    assert cur >= start_date
    assert start_date.floor('D') == cur.floor('D')


@pytest.mark.asyncio
async def test_countstore_measure_noinitstart(monkeypatch):
    """CountStore.start is not set if it contains a non-None value"""
    measure = CountStore()
    measure.start = 42
    called = False

    @measure(name='run')
    async def noop():
        """Do nothing"""
        nonlocal called
        called = True

    assert not measure

    await noop()

    assert called is True
    assert measure.start == 42
    assert measure.start_date is None


@pytest.mark.asyncio
async def test_countstore_measure_failure():
    """Failure is measured"""
    measure = CountStore()
    called = False
    fail = stat.Failure(42)

    @measure(name='run')
    async def noop():
        """Do nothing"""
        nonlocal called
        called = True
        raise fail

    assert not measure

    await noop()

    assert called is True
    count = measure['run']
    assert count.success == 0
    assert not count.error
    assert len(count.failure) == 1
    assert count.failure[str(fail.args[0])] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
async def test_countstore_measure_error(exctype):
    """Errors are measured"""
    measure = CountStore()
    called = False
    err = exctype(42)

    @measure(name='run')
    async def noop():
        """Do nothing"""
        nonlocal called
        called = True
        raise err

    assert not measure

    await noop()

    assert called is True
    count = measure['run']
    assert count.success == 0
    assert not count.failure
    assert len(count.error) == 1
    assert count.error[repr(err)] == 1


# ============================================================================
# Test CountStore.reset()
# ============================================================================


def test_countstore_reset_empty():
    """Does not raise an error on an empty CountStore"""
    c = CountStore()
    c.reset()


def test_countstore_reset_dates():
    """Dates attrs are all set to None"""
    c = CountStore()
    c.start = 42
    c.start_date = now()
    c.end = 52
    c.end_date = now()

    c.reset()

    assert c.start is None
    c.start_date is None
    c.end is None
    c.end_date is None


def test_countstore_reset_clear():
    """The dict is cleared"""
    c = CountStore()
    c['hello'].success = 9001
    c['hello'].error['first'] = 500
    c['hello'].failure['second'] = 500

    c['world'].success = 19001
    c['world'].error['first'] = 1500
    c['world'].failure['second'] = 1500

    assert c
    assert len(c) == 2

    c.reset()

    assert not c
    assert len(c) == 0


# ============================================================================
#
# ============================================================================
