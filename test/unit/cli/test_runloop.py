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
from loadlimit.importhook import mkmodule
from loadlimit.util import Namespace


# ============================================================================
# Fixtures
# ============================================================================


pytestmark = pytest.mark.usefixtures('cleanup', 'fake_shutdown_channel')


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
    __slots__ = ()

    async def __call__(self, state, *, clientid=None):
        state.value += 1

    async def init(self, config, state):
        state.value = 0

    async def shutdown(self, config, state):
        pass
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


def test_runloop(monkeypatch, testloop, modpath):
    """Find tasks in task files and add to loadlimit.task.__tasks__"""
    monkeypatch.setattr(loadlimit.importhook, 'lstaskfiles', fake_lstaskfiles)
    monkeypatch.setattr(loadlimit.importhook, 'SourceFileLoader',
                        FakeSourceFileLoader)

    taskfile = 'a_0.py'
    monkeypatch.setattr(sys, 'argv', [cli.PROGNAME, '-d', '1s',
                                      '--no-progressbar', taskfile])

    config = defaultdict(dict)
    state = Namespace()
    with pytest.raises(SystemExit) as err:
        main(config=config, state=state)

    assert err.value.args == (0, )

    assert state.value > 0
    print(state.value)


# ============================================================================
#
# ============================================================================
