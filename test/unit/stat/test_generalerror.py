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
from pandas import Series, Timestamp, to_timedelta
import pytest

# Local imports
from loadlimit.result import TotalError, TotalFailure
from loadlimit.stat import CountStore, Failure


# ============================================================================
# Test TotalError
# ============================================================================


@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
def test_totalerror_calculate_data(statsdict, exctype):
    """Create dataframe from given error/failure data"""
    measure = CountStore()
    key = '42'
    calc = TotalError(statsdict=statsdict, countstore=measure)
    err = exctype('hello')
    end = Timestamp.now(tz='UTC')
    delta = to_timedelta(5, unit='s')
    data = Series([end, delta, 1/5, repr(err), 1],
                  index=['end', 'rate', 'response', 'error', 'count'])

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
def test_totalerror_calculate_nodata(statsdict, exctype):
    """Create dataframe from given error/failure data"""
    measure = CountStore()
    key = '42'
    calc = TotalError(statsdict=statsdict, countstore=measure)
    err = exctype('hello')
    end = Timestamp.now(tz='UTC')
    delta = to_timedelta(5, unit='s')
    data = Series([end, delta, 1/5, repr(err), 1],
                  index=['end', 'rate', 'response', 'error', 'count'])

    statsdict.adderror(key, data)

    calc.__enter__()
    calc.calculate(key, [], [], [])

    vals = calc.vals
    assert vals
    assert vals.results[key] is None


@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
def test_totalerror_export(monkeypatch, statsdict, exctype):
    """Call exportdf() if there are results"""
    measure = CountStore()
    key = '42'
    calc = TotalError(statsdict=statsdict, countstore=measure)
    err = exctype('hello')
    end = Timestamp.now(tz='UTC')
    delta = to_timedelta(5, unit='s')
    data = Series([end, delta, 1/5, repr(err), 1],
                  index=['end', 'rate', 'response', 'error', 'count'])

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
# Test TotalFailure
# ============================================================================


def test_totalfailure_calculate_data(statsdict):
    """Create dataframe from given failure data"""
    measure = CountStore()
    key = '42'
    calc = TotalFailure(statsdict=statsdict, countstore=measure)
    err = Failure('hello')
    end = Timestamp.now(tz='UTC')
    delta = to_timedelta(5, unit='s')
    data = Series([end, delta, 1/5, str(err.args[0]), 1],
                  index=['end', 'rate', 'response', 'failure', 'count'])

    statsdict.addfailure(key, data)

    calc.__enter__()
    calc.calculate(key, [], [], list(statsdict.failure(key)))

    vals = calc.vals
    assert vals
    assert vals.results[key] is not None

    df = vals.results[key]
    assert df.columns == ['Total']
    assert df.index.names == ['Name', calc.errtype[0].capitalize()]
    assert df.index.values.all() == (key, str(err.args[0]))
    assert df.iloc[0][0] == 1


def test_totalfailure_calculate_nodata(statsdict):
    """Create dataframe from given failure data"""
    measure = CountStore()
    key = '42'
    calc = TotalFailure(statsdict=statsdict, countstore=measure)
    err = Failure('hello')
    end = Timestamp.now(tz='UTC')
    delta = to_timedelta(5, unit='s')
    data = Series([end, delta, 1/5, str(err.args[0]), 1],
                  index=['end', 'rate', 'response', 'failure', 'count'])

    statsdict.addfailure(key, data)

    calc.__enter__()
    calc.calculate(key, [], [], [])

    vals = calc.vals
    assert vals
    assert vals.results[key] is None


@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
def test_totalfailure_export(monkeypatch, statsdict, exctype):
    """Call exportdf() if there are results"""
    measure = CountStore()
    key = '42'
    calc = TotalFailure(statsdict=statsdict, countstore=measure)
    err = Failure('hello')
    end = Timestamp.now(tz='UTC')
    delta = to_timedelta(5, unit='s')
    data = Series([end, delta, 1/5, str(err.args[0]), 1],
                  index=['end', 'rate', 'response', 'failure', 'count'])

    statsdict.addfailure(key, data)

    called = False

    def fake_exportdf(self, df, name, export_type, exportdir):
        nonlocal called
        called = True
        assert df is calc.vals.results
        assert name == calc.errtype[0]
        assert export_type == 'EXPORTTYPE'
        assert exportdir == 'EXPORTDIR'

    monkeypatch.setattr(TotalFailure, 'exportdf', fake_exportdf)

    calc.__enter__()
    calc.calculate(key, [], [], list(statsdict.failure(key)))
    vals = calc.vals
    assert vals
    assert vals.results[key] is not None

    calc.export('EXPORTTYPE', 'EXPORTDIR')
    assert called is True


# ============================================================================
#
# ============================================================================
