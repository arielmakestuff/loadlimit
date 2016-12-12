# -*- coding: utf-8 -*-
# test/unit/stat/test_period.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test Period class"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from asyncio import Lock
from threading import Lock as TLock

# Third-party imports
import pandas as pd
import pytest

# Local imports
from loadlimit.stat import Period
from loadlimit.util import aiter


# ============================================================================
# Test total()
# ============================================================================


def test_total():
    """Returns total number of datapoints in the data structure"""
    p = Period()
    for i in range(5):
        p[i]['timedata'].extend(range(5))

    expected = 25
    assert p.total() == expected
    assert p.numdata == expected


@pytest.mark.asyncio
async def test_atotal():
    """Async version of total()"""
    p = Period()
    async for i in aiter(range(5)):
        p[i]['timedata'].extend(range(5))

    expected = 25
    result = await p.atotal()
    assert result == expected
    assert p.numdata == expected


# ============================================================================
# Test clearvals
# ============================================================================


def test_clearvals_all():
    """Clearvals empties every list in the container"""
    p = Period()
    for i in range(5):
        p[i]['timedata'].extend(range(5))

    p.clearvals()
    assert p.numdata == 0
    for v in p.values():
        assert len(v['timedata']) == 0


def test_clearvals_key():
    """Clearvals empties only the list for the specific key"""
    p = Period()
    for i in range(5):
        p[i]['timedata'].extend(range(5))

    p.clearvals(4)

    assert p.numdata == 20
    for i, v in p.items():
        if i == 4:
            assert len(v['timedata']) == 0
        else:
            assert len(v['timedata']) == 5


# ============================================================================
# Test aclearvals()
# ============================================================================


@pytest.mark.asyncio
async def test_aclearvals_all():
    """Clearvals empties every list in the container"""
    p = Period()
    async for i in aiter(range(5)):
        p[i]['timedata'].extend(range(5))

    await p.aclearvals()

    assert p.numdata == 0
    async for v in aiter(p.values()):
        assert len(v['timedata']) == 0


@pytest.mark.asyncio
async def test_aclearvals_key():
    """Clearvals empties only the list for the specific key"""
    p = Period()
    async for i in aiter(range(5)):
        p[i]['timedata'].extend(range(5))

    await p.aclearvals(4)

    assert p.numdata == 20
    async for i, v in aiter(p.items()):
        if i == 4:
            assert len(v['timedata']) == 0
        else:
            assert len(v['timedata']) == 5


# ============================================================================
# Test period lock
# ============================================================================


def test_period_lockarg():
    """Use custom Lock instance with Period"""
    mylock = Lock()
    p = Period(lock=mylock)
    assert p.lock is mylock


def test_period_defaultlock():
    """Create new Lock object if lock not specified"""
    p = Period()
    assert p.lock
    assert isinstance(p.lock, Lock)
    assert not p.lock.locked()


@pytest.mark.parametrize('obj', [42, 4.2, '42', [42], (4.2, ), TLock])
def test_period_lockarg_notlock(obj):
    """Non- asyncio.Lock objects raises an error"""
    expected = ('lock expected asyncio.Lock, got {} instead'.
                format(type(obj).__name__))
    with pytest.raises(TypeError) as err:
        Period(lock=obj)

    assert err.value.args == (expected, )


# ============================================================================
# Test addtimedata
# ============================================================================


@pytest.mark.parametrize('val', [42, 4.2, '42', [42]])
def test_addtimedata_not_series(val):
    """Raise error if the data arg is not a pandas.Series object"""
    stat = Period()

    expected = ('data expected pandas.Series, got {} instead'.
                format(type(val).__name__))
    with pytest.raises(TypeError) as err:
        stat.addtimedata(42, val)

    assert err.value.args == (expected, )


# ============================================================================
# Test adderror
# ============================================================================


@pytest.mark.parametrize('val', [42, 4.2, '42', [42]])
def test_adderror_not_series(val):
    """Raise error if the data arg is not a pandas.Series object"""
    stat = Period()

    expected = ('data expected pandas.Series, got {} instead'.
                format(type(val).__name__))
    with pytest.raises(TypeError) as err:
        stat.adderror(42, val)

    assert err.value.args == (expected, )


def test_adderror_series():
    """Add a series to the dict"""
    stat = Period()
    error = Exception('i am an error')
    s = pd.Series([1, 1, 0, repr(error)])
    stat.adderror('42', s)

    errors = list(stat.error('42'))
    assert len(errors) == 1
    assert errors[0] is s


# ============================================================================
# Test addfailure
# ============================================================================


@pytest.mark.parametrize('val', [42, 4.2, '42', [42]])
def test_addfailure_not_series(val):
    """Raise error if the data arg is not a pandas.Series object"""
    stat = Period()

    expected = ('data expected pandas.Series, got {} instead'.
                format(type(val).__name__))
    with pytest.raises(TypeError) as err:
        stat.addfailure(42, val)

    assert err.value.args == (expected, )


def test_addfailure_series():
    """Add a series to the dict"""
    stat = Period()
    error = 'i am a failure'
    s = pd.Series([1, 1, 0, error])
    stat.addfailure('42', s)

    failures = list(stat.failure('42'))
    assert len(failures) == 1
    assert failures[0] is s


# ============================================================================
# Test numtimedata
# ============================================================================


@pytest.mark.parametrize('maxnum', list(range(1, 6)))
def test_numtimedata(maxnum):
    """Return number of time data stored"""
    key = 'hello'
    stat = Period()
    for i in range(maxnum):
        s = pd.Series([1, 1, i])
        stat.addtimedata(key, s)
    assert stat.numtimedata(key) == maxnum


# ============================================================================
# Test numerror
# ============================================================================


@pytest.mark.parametrize('maxnum', list(range(1, 6)))
def test_numerror(maxnum):
    """Return number of errors stored"""
    key = 'hello'
    stat = Period()
    err = Exception(key)
    for i in range(maxnum):
        s = pd.Series([1, 1, i, repr(err)])
        stat.adderror(key, s)
    assert stat.numerror(key) == maxnum


# ============================================================================
# Test numfailure
# ============================================================================


@pytest.mark.parametrize('maxnum', list(range(1, 6)))
def test_numfailure(maxnum):
    """Return number of failures stored"""
    key = 'hello'
    err = 'world'
    stat = Period()
    for i in range(maxnum):
        s = pd.Series([1, 1, i, err])
        stat.addfailure(key, s)
    assert stat.numfailure(key) == maxnum


# ============================================================================
#
# ============================================================================
