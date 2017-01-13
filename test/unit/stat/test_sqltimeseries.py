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
from pandas import DataFrame

# Local imports
from loadlimit.result import SQLTimeSeries
from loadlimit.stat import CountStore
from loadlimit.util import Namespace


# ============================================================================
# Test
# ============================================================================


def test_calculate_nodata(statsdict):
    """Set results for a key to None if no data"""
    measure = CountStore()
    key = '42'
    state = Namespace()
    calc = SQLTimeSeries(state, statsdict=statsdict, countstore=measure)
    empty = DataFrame()
    calc.__enter__()
    calc.calculate(key, empty, empty, empty)
    vals = calc.vals
    assert vals.response_result[key] is None
    assert vals.rate_result[key] is None


# ============================================================================
#
# ============================================================================
