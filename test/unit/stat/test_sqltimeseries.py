# -*- coding: utf-8 -*-
# test/unit/stat/test_sqltimeseries.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test SQLTimeSeries"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports

# Third-party imports

# Local imports
from loadlimit.stat import SQLTimeSeries


# ============================================================================
# Test
# ============================================================================


def test_calculate_nodata(statsdict):
    """Set results for a key to None if no data"""
    key = '42'
    calc = SQLTimeSeries(statsdict=statsdict)
    calc.vals.periods = 3
    calc.__enter__()
    calc.calculate(key, [], [], [])
    vals = calc.vals
    assert vals.response_result[key] is None
    assert vals.rate_result[key] is None


# ============================================================================
#
# ============================================================================
