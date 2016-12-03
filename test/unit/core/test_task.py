# -*- coding: utf-8 -*-
# test/unit/core/test_task.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test Task"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio

# Third-party imports
import pytest

# Local imports
from loadlimit.core import Task


# ============================================================================
# Test error
# ============================================================================


@pytest.mark.parametrize('val', [42, 4.2, '42', [], (), {}, list])
def test_task_badvalue(val):
    """Raise error if given a non-coroutine function"""
    expected = ('corofunc expected coroutine function, got {} instead'.
                format(type(val).__name__))

    with pytest.raises(TypeError) as err:
        Task(val)

    assert err.value.args == (expected, )


# ============================================================================
# Test wraps coroutine func
# ============================================================================


def test_task_wrap_corofunc(testloop):
    """Wraps corofunc"""
    val = []

    async def one():
        """one"""
        val.append(1)

    async def two():
        """two"""
        val.append(2)

    tasks = [Task(f) for f in [one, two]]

    # Init each task
    f = asyncio.gather(*[t.init(None, None) for t in tasks])
    testloop.run_until_complete(f)
    assert not val

    # Run each task
    f = asyncio.gather(*[t(None) for t in tasks])
    testloop.run_until_complete(f)

    # Shutdown each task
    f = asyncio.gather(*[t.shutdown(None, None) for t in tasks])
    testloop.run_until_complete(f)

    assert set(val) == set([1, 2])


# ============================================================================
#
# ============================================================================
