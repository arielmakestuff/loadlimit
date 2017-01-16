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

# Third-party imports
import pytest

# Local imports
from loadlimit.core import Client, Task, TaskABC


# ============================================================================
# Test init
# ============================================================================


def test_init_noargs():
    """Raise error if no args given"""
    expected = 'Client object did not receive any TaskABC subclasses'
    with pytest.raises(ValueError) as err:
        Client()

    assert err.value.args == (expected, )


@pytest.mark.parametrize('val', ['42', 4.2, list])
def test_init_badargs(val):
    """Raise error if given a non coroutine callable"""

    expected = ('cf_or_cfiter expected TaskABC subclass, got {} instead'.
                format(type(val).__name__))
    with pytest.raises(TypeError) as err:
        Client(val)

    assert err.value.args == (expected, )


def test_init_badargs_iterable():
    """Raise error if given iterable with non coroutine callable"""
    val = [4.2]
    expected = ('cf_or_cfiter expected TaskABC subclass, got {} instead'.
                format(type(val[0]).__name__))
    with pytest.raises(TypeError) as err:
        Client(val)

    assert err.value.args == (expected, )


def test_init_mixedargs(testloop):
    """Accepts mix of good args"""

    async def one():
        """one"""

    async def two():
        """two"""

    async def three():
        """three"""

    async def four():
        """four"""

    class Five(TaskABC):
        """five"""
        __slots__ = ()

        async def __call__(self, state, *, clientid=None):
            """call"""

        async def init(self, config, state):
            """init"""

        async def shutdown(self, config, state):
            """shutdown"""

    c = Client(Task(one), [Task(two), Task(three)], Task(four), [Five])
    testloop.run_until_complete(c(None))


def test_init_id():
    """Return the client's unique id number"""

    async def one():
        pass

    c = Client(Task(one))

    assert c.id == id(c)


# ============================================================================
# Test __call__
# ============================================================================


def test_init_call(testloop):
    """Schedules all given coroutines and waits for them to finish"""

    val = []

    class TestClient(Client):
        """Add value once all tasks finished"""

        async def __call__(self, state, *, clientid=None):
            await super().__call__(state, clientid=clientid)
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

    tasks = [Task(cf) for cf in [one, two, three]]
    c = TestClient(tasks)

    # Init tasks
    testloop.run_until_complete(c.init(None, None))

    # Run client
    testloop.run_until_complete(c(None))

    assert val
    assert len(val) == 4
    assert set(val[:-1]) == set([1, 2, 3])
    assert val[-1] == 9000


# ============================================================================
# Test reschedule
# ============================================================================


class RunClient5(Client):
    """Client that 5 times"""

    def __init__(self, *args):
        super().__init__(*args, reschedule=True)
        self._count = 0

    @property
    def option(self):
        """Increment the count every time this is accessed"""
        option = super().option
        self._count = self._count + 1
        if self._count == 5:
            option.reschedule = False
        return option


def test_reschedule(testloop):
    """Client keeps running if reschedule is True"""

    val = []

    async def one():
        """one"""
        val.append(1)

    c = RunClient5(Task(one))

    # Init tasks
    testloop.run_until_complete(c.init(None, None))

    # Run client
    testloop.run_until_complete(c(None))

    assert val
    assert len(val) == 5
    assert val == [1] * 5


# ============================================================================
#
# ============================================================================
