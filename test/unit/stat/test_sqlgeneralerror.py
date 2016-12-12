# -*- coding: utf-8 -*-
# test/unit/stat/test_sqlgeneralerror.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test SQLGeneralError"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports

# Third-party imports
from pandas import DataFrame, Series
import pytest

# Local imports
import loadlimit.stat as stat
from loadlimit.stat import SQLTotalError


# ============================================================================
# Tests
# ============================================================================


@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
def test_calculate(statsdict, exctype):
    """Create dataframe from given error/failure data"""
    key = '42'
    calc = SQLTotalError(statsdict=statsdict)
    err = exctype('hello')
    end = statsdict.end_date
    start = statsdict.start_date
    delta = end - start
    data = Series([start, end, delta, repr(err)],
                  index=['start', 'end', 'delta', 'error'])
    datadf = DataFrame([data], index=[0],
                       columns=['start', 'end', 'delta', 'error'])

    calc.__enter__()
    calc.calculate(key, [], datadf, [])

    vals = calc.vals
    assert vals
    assert vals.results[key] is not None

    df = vals.results[key]
    assert df.columns == ['Total']
    assert df.index.names == ['Name', calc.errtype[0].capitalize()]
    assert df.index.values.all() == (key, repr(err))
    assert df.iloc[0][0] == 1


# ============================================================================
#
# ============================================================================
