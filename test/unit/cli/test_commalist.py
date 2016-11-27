# -*- coding: utf-8 -*-
# test/unit/cli/test_commalist.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test commalist()"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports

# Third-party imports
import pytest

# Local imports
from loadlimit.cli import commalist


# ============================================================================
# Tests
# ============================================================================


@pytest.mark.parametrize('val', [None, '', [], ()])
def test_commalist_empty(val):
    """Return an empty list if given a value that boolean evals to False"""
    assert commalist(val) == []


@pytest.mark.parametrize('val', [42, 4.2, ['42']])
def test_commalist_nonstr(val):
    """Non-str values raises an error"""
    expected = ("'{}' object has no attribute 'split'".
                format(type(val).__name__))
    with pytest.raises(AttributeError) as err:
        commalist(val)

    assert err.value.args == (expected, )


@pytest.mark.parametrize('val', ['a', 'a,b', 'a,b,c'])
def test_commalist(val):
    """Convert a comma-delimited string to a list of strings"""
    expected = val.split(',')
    assert commalist(val) == expected


# ============================================================================
#
# ============================================================================
