# -*- coding: utf-8 -*-
# test/unit/core/conftest.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Configuration for core tests"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import sys

# Third-party imports
import pytest

# Local imports
from loadlimit.importhook import TaskImporter


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def modpath():
    """Return the virtual task python path"""
    return 'loadlimit.task'


@pytest.yield_fixture
def cleanup(modpath):
    """Remove dummy modules from sys.modules"""
    yield

    # Remove dummy modules from sys.modules
    pathlist = [p for p in sys.modules
                if p != modpath and p.startswith(modpath)]

    for p in pathlist:
        del sys.modules[p]

    if modpath in sys.modules:
        del sys.modules[modpath]

    # Remove TaskImporter
    index = [i for i, obj in enumerate(sys.meta_path)
             if isinstance(obj, TaskImporter)]

    for i in reversed(index):
        assert isinstance(sys.meta_path[i], TaskImporter)
        sys.meta_path.pop(i)


# ============================================================================
#
# ============================================================================
