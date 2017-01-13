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
from pandas import DataFrame, Timestamp, to_timedelta

# Local imports
from loadlimit.result import SQLTotal
from loadlimit.stat import CountStore
from loadlimit.util import Namespace


# ============================================================================
# Test
# ============================================================================


def test_calculate_nodata(statsdict):
    """Set results for a key to None if no data"""
    measure = CountStore()
    measure.start_date = s = Timestamp.now(tz='UTC')
    measure.end_date = s + to_timedelta(5, unit='s')
    key = '42'
    state = Namespace()
    calc = SQLTotal(state, statsdict=statsdict, countstore=measure)
    empty = DataFrame()
    calc.__enter__()
    calc.calculate(key, empty, empty, empty)
    results = calc.vals.results
    assert results
    assert results[key] is None


# ============================================================================
#
# ============================================================================
