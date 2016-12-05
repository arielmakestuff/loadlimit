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
#
# ============================================================================
