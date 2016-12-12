# -*- coding: utf-8 -*-
# test/unit/stat/test_generalerror.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test GeneralError"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports

# Third-party imports
from pandas import Series
import pytest

# Local imports
import loadlimit.stat as stat
from loadlimit.stat import TotalError, TotalFailure


# ============================================================================
# Fixtures
# ============================================================================


# ============================================================================
# Tests
# ============================================================================


@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
def test_calculate(statsdict, exctype):
    """Create dataframe from given error/failure data"""
    key = '42'
    calc = TotalError(statsdict=statsdict)
    err = exctype('hello')
    end = statsdict.end_date
    start = statsdict.start_date
    delta = end - start
    data = Series([start, end, delta, repr(err)],
                  index=['start', 'end', 'delta', 'error'])

    statsdict.adderror(key, data)

    calc.__enter__()
    calc.calculate(key, [], list(statsdict.error(key)), [])

    vals = calc.vals
    assert vals
    assert vals.results[key] is not None

    df = vals.results[key]
    assert df.columns == ['Total']
    assert df.index.names == ['Name', calc.errtype[0].capitalize()]
    assert df.index.values.all() == (key, repr(err))
    assert df.iloc[0][0] == 1


@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
def test_calculate_nodata(statsdict, exctype):
    """Create dataframe from given error/failure data"""
    key = '42'
    calc = TotalError(statsdict=statsdict)
    err = exctype('hello')
    end = statsdict.end_date
    start = statsdict.start_date
    delta = end - start
    data = Series([start, end, delta, repr(err)],
                  index=['start', 'end', 'delta', 'error'])

    statsdict.adderror(key, data)

    calc.__enter__()
    calc.calculate(key, [], [], [])

    vals = calc.vals
    assert vals
    assert vals.results[key] is None


# ============================================================================
# Test export
# ============================================================================


@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
def test_export(monkeypatch, statsdict, exctype):
    """Call exportdf() if there are results"""

    key = '42'
    calc = TotalError(statsdict=statsdict)
    err = exctype('hello')
    end = statsdict.end_date
    start = statsdict.start_date
    delta = end - start
    data = Series([start, end, delta, repr(err)],
                  index=['start', 'end', 'delta', 'error'])

    statsdict.adderror(key, data)

    called = False

    def fake_exportdf(self, df, name, export_type, exportdir):
        nonlocal called
        called = True
        assert df is calc.vals.results
        assert name == calc.errtype[0]
        assert export_type == 'EXPORTTYPE'
        assert exportdir == 'EXPORTDIR'

    monkeypatch.setattr(TotalError, 'exportdf', fake_exportdf)

    calc.__enter__()
    calc.calculate(key, [], list(statsdict.error(key)), [])
    vals = calc.vals
    assert vals
    assert vals.results[key] is not None

    calc.export('EXPORTTYPE', 'EXPORTDIR')
    assert called is True


# ============================================================================
#
# ============================================================================
