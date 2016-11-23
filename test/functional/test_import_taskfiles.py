# -*- coding: utf-8 -*-
# test/unit/importhook/test_taskimporter.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test taskfilematch()"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from os.path import splitext
from random import choice
import sys
from importlib import import_module

# Third-party imports
import pytest

# Local imports
import loadlimit.importhook
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
# Auto-fixture
# ============================================================================


pytestmark = pytest.mark.usefixtures('cleanup')


# ============================================================================
# Experiments
# ============================================================================


def test_import_datadir(modpath, datadir):
    """Import all task files in taskdir"""
    taskdir = str(datadir / 'import_taskfiles')
    sys.meta_path.append(TaskImporter(taskdir=taskdir))
    task = import_module(modpath)
    assert hasattr(task, '__taskmodules__')

    answer = None
    for modpath in task.__taskmodules__:
        modname = modpath.rpartition('.')[2]
        if modname.endswith('_42'):
            answer = getattr(task, modname).ANSWER
            break

    assert answer == 42


# ============================================================================
#
# ============================================================================
