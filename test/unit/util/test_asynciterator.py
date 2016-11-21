# -*- coding: utf-8 -*-
# test/unit/util/test_asynciterator.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test AsyncIterator"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports

# Third-party imports
import pytest

# Local imports
from loadlimit.util import aiter


# ============================================================================
# Test
# ============================================================================


@pytest.mark.asyncio
async def test_aiter():
    """aiter wraps normal iterators"""
    val = []
    expected = list(range(10))
    async for i in aiter(range(10)):
        val.append(i)

    assert val == expected


# ============================================================================
#
# ============================================================================
