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
from pandas import DataFrame, Series, Timestamp, to_timedelta
import pytest

# Local imports
from loadlimit.result import SQLTotalError, SQLTotalFailure
from loadlimit.stat import TimelineFrame, Failure


# ============================================================================
# Test SQLTotalError
# ============================================================================


@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
def test_sqltotalerror_calculate(statsdict, exctype):
    """Create dataframe from given error data"""
    measure = TimelineFrame()
    key = '42'
    calc = SQLTotalError(statsdict=statsdict, countstore=measure)
    err = exctype('hello')
    end = Timestamp.now(tz='UTC')
    delta = to_timedelta(5, unit='s')
    data = Series([end, delta, 1/5, repr(err), 1],
                  index=['end', 'rate', 'response', 'error', 'count'])
    datadf = DataFrame([data], index=[0], columns=data.index)

    calc.__enter__()
    calc.calculate(key, [], datadf, [])

    vals = calc.vals
    assert vals
    assert vals.results[key] is not None

    df = vals.results[key]
    assert df.columns == ['Total']
    assert df.index.names == ['Name', 'Error']
    assert df.index.values.all() == (key, repr(err))
    assert df.iloc[0][0] == 1


# ============================================================================
# Test SQLTotalFailure
# ============================================================================


def test_sqltotalfailure_calculate(statsdict):
    """Create dataframe from given failure data"""
    measure = TimelineFrame()
    key = '42'
    calc = SQLTotalFailure(statsdict=statsdict, countstore=measure)
    err = Failure('hello')
    end = Timestamp.now(tz='UTC')
    delta = to_timedelta(5, unit='s')
    data = Series([end, delta, 1/5, str(err.args[0]), 1],
                  index=['end', 'rate', 'response', 'failure', 'count'])
    datadf = DataFrame([data], index=[0], columns=data.index)

    calc.__enter__()
    calc.calculate(key, [], [], datadf)

    vals = calc.vals
    assert vals
    assert vals.results[key] is not None

    df = vals.results[key]
    assert df.columns == ['Total']
    assert df.index.names == ['Name', 'Failure']
    assert df.index.values.all() == (key, str(err.args[0]))
    assert df.iloc[0][0] == 1


# ============================================================================
#
# ============================================================================
