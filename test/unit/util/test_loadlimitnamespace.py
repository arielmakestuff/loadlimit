# -*- coding: utf-8 -*-
# test/unit/util/test_loadlimitnamespace.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.
"""Test Namespace class"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports

# Third-party imports

# Local imports
from loadlimit.util import Namespace


# ============================================================================
# Test LoadLimitNamespace
# ============================================================================


def test_empty():
    """Eval as False if ns is empty"""
    ns = Namespace()
    assert bool(ns) is False
    assert not ns


def test_notempty():
    """Eval as True if ns has attributes"""
    kwargs = {v: i+1 for i, v in
              enumerate(['one', 'two', 'three', 'four', 'five'])}
    ns = Namespace(**kwargs)
    assert bool(ns) is True
    assert ns


# ============================================================================
#
# ============================================================================
