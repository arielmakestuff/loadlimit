# -*- coding: utf-8 -*-
# test/unit/stat/test_timecoro.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test timecoro decorator"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from asyncio import iscoroutinefunction

# Third-party imports
import pytest

# Local imports
import loadlimit.stat as stat
from loadlimit.stat import Failure, timecoro


# ============================================================================
# Fixtures
# ============================================================================


pytestmark = pytest.mark.usefixtures('fake_timedata_channel')


# ============================================================================
# Test total()
# ============================================================================


def test_timecoro_noname():
    """Raise error if timecoro is not given a name"""

    expected = 'name not given'
    with pytest.raises(ValueError) as err:
        timecoro()

    assert err.value.args == (expected, )


@pytest.mark.parametrize('val', [42, 4.2, [42], {}, ()])
def test_timecoro_name_notstr(val):
    """Raise error if timecoro is given a non-str name"""
    expected = 'name expected str, got {} instead'.format(type(val).__name__)
    with pytest.raises(TypeError) as err:
        timecoro(name=val)

    assert err.value.args == (expected, )


def test_timecoro_decorator():
    """Calling immediately with corofunc"""

    async def one():
        """one"""

    wrapped = timecoro(one, name='one')

    assert wrapped is not one
    assert iscoroutinefunction(wrapped)
    assert hasattr(wrapped, '__wrapped__')
    assert wrapped.__wrapped__ is one


# ============================================================================
# Test catching Failure exception
# ============================================================================


@pytest.mark.parametrize('val', [42, 4.2, 'hello'])
def test_catch_failure(val, testloop):
    """Catch raised Failure exceptions"""
    timedata = stat.timedata

    @timecoro(name='hello')
    async def coro():
        raise Failure(val)

    async def runme():
        await coro()

    @timedata
    async def catchdata(data):
        assert data.failure == str(val)
        await timedata.shutdown()

    timedata.open()
    timedata.start()
    testloop.run_until_complete(runme())


# ============================================================================
# Test catching non-Failure exception
# ============================================================================


@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
def test_catch_error(exctype, testloop):
    """Catch raised exceptions"""
    statsdict = stat.Period()
    timedata = stat.timedata
    err = exctype(42)
    called_catchdata = False

    @timecoro(name='hello')
    async def coro():
        raise err

    async def runme():
        await coro()
        await timedata.join()

    @timedata
    async def catchdata(data, **kwargs):
        nonlocal called_catchdata
        called_catchdata = True
        assert data.error == err

    timedata.open()
    timedata.start(statsdict=statsdict)
    testloop.run_until_complete(runme())
    assert called_catchdata is True


# ============================================================================
#
# ============================================================================
