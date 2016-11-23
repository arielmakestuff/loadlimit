# -*- coding: utf-8 -*-
# test/unit/importhook/test_taskfilematch.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test taskfilematch()"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports

# Third-party imports
import pytest

# Local imports
import loadlimit.importhook


# ============================================================================
# Globals
# ============================================================================


taskfilematch = loadlimit.importhook.taskfilematch


# ============================================================================
# Test taskfilematch
# ============================================================================


@pytest.mark.parametrize('name', ['hello', 'world.py'])
def test_disabled_filename(monkeypatch, name):
    """Return None if filename ends with .disabled"""

    def fake_isfile(filename):
        return True

    monkeypatch.setattr(loadlimit.importhook, 'isfile', fake_isfile)

    n = '{}.disabled'.format(name)
    assert taskfilematch(n) is None


@pytest.mark.parametrize('name', ['hello', 'world.py', 'what',
                                  'now.py.disabled'])
def test_notfile_filename(monkeypatch, name):
    """Return None if filename ends with .disabled"""

    def fake_isfile(filename):
        return False

    monkeypatch.setattr(loadlimit.importhook, 'isfile', fake_isfile)

    n = '{}'.format(name)
    assert taskfilematch(n) is None


# ============================================================================
#
# ============================================================================
