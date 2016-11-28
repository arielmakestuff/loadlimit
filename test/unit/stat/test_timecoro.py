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
from loadlimit.stat import timecoro


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
#
# ============================================================================
