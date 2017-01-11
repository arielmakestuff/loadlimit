# -*- coding: utf-8 -*-
# test/unit/stat/test_total.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test Total"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports

# Third-party imports
from pandas import Timestamp, to_timedelta

# Local imports
import loadlimit.result as result
import loadlimit.stat as stat


# ============================================================================
# Test no results
# ============================================================================


def test_noresults(statsdict):
    """Set results to None if no results"""
    measure = stat.CountStore()
    measure.start_date = s = Timestamp.now(tz='UTC')
    measure.end_date = s + to_timedelta(5, unit='s')
    with result.Total(statsdict=statsdict, countstore=measure) as r:
        pass

    assert r.vals.results is None


# ============================================================================
# Test calculate w/ no data
# ============================================================================


def test_calculate_nodata(statsdict):
    """Set results for a key to None if no data"""
    measure = stat.CountStore()
    measure.start_date = s = Timestamp.now(tz='UTC')
    measure.end_date = s + to_timedelta(5, unit='s')
    key = '42'
    calc = result.Total(statsdict=statsdict, countstore=measure)
    calc.__enter__()
    calc.calculate(key, [], [], [])

    results = calc.vals.results
    assert results
    assert results[key] is None


# ============================================================================
# Test export
# ============================================================================


def test_export_nodata(monkeypatch, statsdict):
    """Do not call exportdf() if there are no results"""
    measure = stat.CountStore()
    measure.start_date = s = Timestamp.now(tz='UTC')
    measure.end_date = s + to_timedelta(5, unit='s')
    calc = result.Total(statsdict=statsdict, countstore=measure)
    called = False

    def fake_exportdf(self, df, name, export_type, exportdir):
        nonlocal called
        called = True

    monkeypatch.setattr(result.Total, 'exportdf', fake_exportdf)

    with calc:
        pass
    assert calc.vals.results is None

    calc.export('EXPORTTYPE', 'EXPORTDIR')
    assert called is False


# ============================================================================
#
# ============================================================================
