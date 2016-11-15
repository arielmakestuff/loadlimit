# Module:
# Submodules:
# Created:
# Copyright (C) <date> <fullname>
#
# This module is part of the <project name> project and is released under
# the MIT License: http://opensource.org/licenses/MIT
"""
"""

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
