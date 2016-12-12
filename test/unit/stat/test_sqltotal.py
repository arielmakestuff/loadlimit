# -*- coding: utf-8 -*-
# test/unit/stat/test_sqltotal.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test SQLTotal"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports

# Third-party imports

# Local imports
from loadlimit.stat import SQLTotal


# ============================================================================
# Test
# ============================================================================


def test_calculate_nodata(statsdict):
    """Set results for a key to None if no data"""
    key = '42'
    calc = SQLTotal(statsdict=statsdict)
    calc.__enter__()
    calc.calculate(key, [], [], [])
    results = calc.vals.results
    assert results
    assert results[key] is None


# ============================================================================
#
# ============================================================================
