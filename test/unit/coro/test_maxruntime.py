# -*- coding: utf-8 -*-
# test/unit/coro/test_maxruntime.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test maxruntime() coro func"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from datetime import timedelta

# Third-party imports
from pandas import Timedelta
import pytest

# Local imports
from loadlimit.core import BaseLoop
from loadlimit.coro import maxruntime


# ============================================================================
# Tests
# ============================================================================


@pytest.mark.parametrize('cls', [Timedelta, timedelta])
def test_stops_event(testloop, cls):
    """Stops the event loop after the specified amount of time"""

    val = 0

    async def run():
        """run"""
        nonlocal val
        while True:
            val = val + 1
            await asyncio.sleep(0.1)

    end = cls(seconds=1)

    with BaseLoop() as main:
        for coro in [maxruntime(end), run()]:
            asyncio.ensure_future(coro)
        main.start()

    assert val >= 10


@pytest.mark.parametrize('val', [42, 4.2, '42', [42], (42, )])
@pytest.mark.asyncio
async def test_not_timedelta(val):
    """Raises error if delta arg is not a timedelta instance"""

    expected = ('delta expected timedelta, got {} instead'.
                format(type(val).__name__))
    with pytest.raises(TypeError) as err:
        await maxruntime(val)

    assert err.value.args == (expected, )


# ============================================================================
#
# ============================================================================
