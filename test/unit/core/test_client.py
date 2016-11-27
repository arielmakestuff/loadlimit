# -*- coding: utf-8 -*-
# test/unit/core/test_client.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test client"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio

# Third-party imports
import pytest

# Local imports
from loadlimit.core import Client


# ============================================================================
# Test init
# ============================================================================


def test_init_noargs():
    """Raise error if no args given"""
    expected = 'Client object did not receive any coroutine callables'
    with pytest.raises(ValueError) as err:
        Client()

    assert err.value.args == (expected, )


@pytest.mark.parametrize('val', ['42', 4.2, list])
def test_init_badargs(val):
    """Raise error if given a non coroutine callable"""

    expected = ('cf_or_cfiter expected coroutine callable, got {} instead'.
                format(type(val).__name__))
    with pytest.raises(TypeError) as err:
        Client(val)

    assert err.value.args == (expected, )


def test_init_badargs_iterable():
    """Raise error if given iterable with non coroutine callable"""
    val = [4.2]
    expected = ('cf_or_cfiter expected coroutine callable, got {} instead'.
                format(type(val[0]).__name__))
    with pytest.raises(TypeError) as err:
        Client(val)

    assert err.value.args == (expected, )


def test_init_mixedargs():
    """Accepts mix of good args"""

    async def one():
        """one"""

    async def two():
        """two"""

    async def three():
        """three"""

    async def four():
        """four"""

    class Five:
        """five"""

        async def __call__(self):
            """call"""

    Client(one, [two, three], four, [Five])


# ============================================================================
# Test __call__
# ============================================================================


def test_init_call(testloop):
    """Schedules all given coroutines and waits for them to finish"""

    val = []

    class TestClient(Client):
        """Add value once all tasks finished"""

        async def __call__(self):
            await super().__call__()
            val.append(9000)

    async def one():
        """one"""
        val.append(1)

    async def two():
        """two"""
        val.append(2)

    async def three():
        """three"""
        val.append(3)

    c = TestClient(one, two, three)
    t = [asyncio.ensure_future(coro) for coro in [c.init(None), c()]]
    f = asyncio.gather(*t)
    testloop.run_until_complete(f)

    assert val
    assert len(val) == 4
    assert set(val[:-1]) == set([1, 2, 3])
    assert val[-1] == 9000


# ============================================================================
#
# ============================================================================
