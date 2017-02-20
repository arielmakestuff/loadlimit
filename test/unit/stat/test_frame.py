# -*- coding: utf-8 -*-
# test/unit/stat/test_frame.py
# Copyright (C) 2017 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Tests for Frame"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from collections import Counter, defaultdict

# Third-party imports
# from pandas import Timestamp
import pytest

# Local imports
# import loadlimit.stat as stat
from loadlimit.stat import (Frame)
# from loadlimit.util import now


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def frame():
    """Create a new frame"""
    return Frame()


# ============================================================================
# Test __init__
# ============================================================================


def test_init_attr_values(frame):
    """Set initial attr values"""
    # --------------------
    # Test
    # --------------------
    # Start and end
    assert frame.start is None
    assert frame.end is None

    # Client set
    assert isinstance(frame.client, set)
    assert len(frame.client) == 0

    # Counters
    assert isinstance(frame.success, Counter)
    assert isinstance(frame.error, defaultdict)
    assert isinstance(frame.failure, defaultdict)
    assert not frame.success
    assert not frame.error
    assert not frame.failure

    assert frame.error.default_factory == Counter
    assert frame.failure.default_factory == Counter


# ============================================================================
# Test sum
# ============================================================================


def test_sum_nocounts(frame):
    """Return 0 if no counts"""
    # --------------------
    # Test
    # --------------------
    assert frame.sum() == 0


@pytest.mark.parametrize('maxlen', list(range(5)))
def test_sum_success_count(frame, maxlen):
    """Return sum of success counts if no error or failure counts"""
    # --------------------
    # Setup
    # --------------------
    counts = list(range(maxlen))
    names = ['a{}'.format(i) for i in counts]
    for name, c in zip(names, counts):
        frame.success[name] = c

    # --------------------
    # Test
    # --------------------
    assert frame.sum() == sum(counts)


@pytest.mark.parametrize('maxlen', list(range(5)))
def test_sum_error_count(frame, maxlen):
    """Return sum of error counts if no success or failure counts"""
    # --------------------
    # Setup
    # --------------------
    counts = list(range(maxlen))
    names = ['a{}'.format(i) for i in counts]
    errnames = ['e{}'.format(i) for i in counts]
    for name in names:
        for err, c in zip(errnames, counts):
            frame.error[name][err] = c

    # --------------------
    # Test
    # --------------------
    assert not frame.success
    assert not frame.failure
    assert frame.error if maxlen else not frame.error

    assert frame.sum() == sum(counts) * maxlen


@pytest.mark.parametrize('maxlen', list(range(5)))
def test_sum_failure_count(frame, maxlen):
    """Return sum of failure counts if no success or error counts"""
    # --------------------
    # Setup
    # --------------------
    counts = list(range(maxlen))
    names = ['a{}'.format(i) for i in counts]
    failnames = ['f{}'.format(i) for i in counts]
    for name in names:
        for fail, c in zip(failnames, counts):
            frame.failure[name][fail] = c

    # --------------------
    # Test
    # --------------------
    assert not frame.success
    assert not frame.error
    assert frame.failure if maxlen else not frame.failure

    assert frame.sum() == sum(counts) * maxlen


@pytest.mark.parametrize('maxlen', list(range(5)))
def test_sum_all_counts(frame, maxlen):
    """Return sum of all counts"""
    # --------------------
    # Setup
    # --------------------
    counts = list(range(maxlen))
    names = ['a{}'.format(i) for i in counts]
    errnames = ['e{}'.format(i) for i in counts]
    failnames = ['f{}'.format(i) for i in counts]
    for name, sc in zip(names, counts):
        frame.success[name] = sc
        for err, c in zip(errnames, counts):
            frame.error[name][err] = c
        for fail, c in zip(failnames, counts):
            frame.failure[name][fail] = c

    # --------------------
    # Test
    # --------------------
    assert frame.success if maxlen else not frame.success
    assert frame.error if maxlen else not frame.error
    assert frame.failure if maxlen else not frame.failure

    expected = sum(counts) + (sum(counts) * maxlen * 2)
    assert frame.sum() == expected


# ============================================================================
#
# ============================================================================
