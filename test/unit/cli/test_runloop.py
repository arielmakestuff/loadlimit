# -*- coding: utf-8 -*-
# test/unit/core/test_runloop.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test runloop"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from collections import defaultdict
from os.path import splitext
import sys

# Third-party imports
import pytest

# Local imports
import loadlimit.cli as cli
from loadlimit.cli import main
import loadlimit.importhook
from loadlimit.importhook import TaskImporter, mkmodule


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


pytestmark = pytest.mark.usefixtures('cleanup')


# ============================================================================
# Helpers
# ============================================================================


class FakeSourceFileLoader:
    """Fake SourceFileLoader"""

    def __init__(self, name, path):
        self._fake_name = name

    def load_module(self, name):
        """Fake load_module"""
        src = self.mksrc()
        return mkmodule(src, name)

    def exec_module(self, module):
        """Fake exec_module"""
        src = self.mksrc()
        c = compile(src, '', 'exec')
        exec(c, module.__dict__)

    def mksrc(self):
        """Make source code"""
        src = """
from loadlimit.core import TaskABC

class TestTask(TaskABC):

    def __init__(self):
        self._testrun = None

    async def __call__(self):
        self._testrun['value'] += 1

    async def init(self, config):
        config['testrun']['value'] = 0
        self._testrun = config['testrun']
        """.strip()
        return src


def fake_lstaskfiles(*taskfiles, taskdir=None, checkerr=False):
    """Files"""
    ret = [tuple(splitext(t)) + (t, ) for t in taskfiles]
    return ret


def fake_sysexit(exitcode):
    """fake_sysexit"""
    pass


# ============================================================================
# Tests
# ============================================================================


def test_runloop(monkeypatch, modpath):
    """Find tasks in task files and add to loadlimit.task.__tasks__"""
    monkeypatch.setattr(loadlimit.importhook, 'lstaskfiles', fake_lstaskfiles)
    monkeypatch.setattr(loadlimit.importhook, 'SourceFileLoader',
                        FakeSourceFileLoader)

    taskfile = 'a_0.py'
    monkeypatch.setattr(sys, 'argv', [cli.PROGNAME, '-d', '1s', taskfile])

    config = defaultdict(dict)
    with pytest.raises(SystemExit) as err:
        main(config=config)

    assert err.value.args == (0, )

    assert 'testrun' in config
    assert config['testrun'].get('value', 0) > 0


# ============================================================================
#
# ============================================================================
