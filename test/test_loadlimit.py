# -*- coding: utf-8 -*-
# loadlimit/test/test_loadlimit.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Example test"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports

# Third-party imports
import pytest

# Local imports


# ============================================================================
# Function
# ============================================================================


def fib(n):
    """
    Fibonacci example function

    :param n: integer
    :return: n-th Fibonacci number
    """
    assert n > 0
    a, b = 1, 1
    for i in range(n-1):
        a, b = b, a+b
    return a


# ============================================================================
# Test
# ============================================================================


def test_fib():
    """Test fib()"""
    assert fib(1) == 1
    assert fib(2) == 1
    assert fib(7) == 13
    with pytest.raises(AssertionError):
        fib(-10)


# ============================================================================
#
# ============================================================================
