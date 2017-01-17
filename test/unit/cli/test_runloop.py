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
import asyncio
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

    def create_module(self, spec):
        """Fake create_module"""

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


@pytest.yield_fixture
def fake_proactorloop(monkeypatch):

    def fake_proactor_event_loop():
        return 42

    if sys.platform == 'win32':
        monkeypatch.setattr(asyncio, 'ProactorEventLoop', fake_proactor_event_loop)
        yield
    else:
        monkeypatch.setattr(sys, 'platform', 'win32')
        asyncio.ProactorEventLoop = fake_proactor_event_loop
        yield
        delattr(asyncio, 'ProactorEventLoop')


@pytest.mark.usefixtures('fake_proactorloop')
def test_use_proactoreventloop(monkeypatch):
    """Test that ProActorEventLoop is created"""

    class StopTest(Exception):
        pass

    monkeypatch.setattr(loadlimit.importhook, 'lstaskfiles', fake_lstaskfiles)
    monkeypatch.setattr(loadlimit.importhook, 'SourceFileLoader',
                        FakeSourceFileLoader)

    def fake_set_event_loop(l):
        nonlocal loopval
        loopval = l
        raise StopTest

    def fake_processoptions(c, s):
        raise RuntimeError

    monkeypatch.setattr(asyncio, 'set_event_loop', fake_set_event_loop)
    monkeypatch.setattr(cli, 'process_options', fake_processoptions)

    taskfile = 'a_0.py'
    config = defaultdict(dict)
    state = Namespace()
    args = [cli.PROGNAME, '-d', '1s', '--no-progressbar', taskfile]
    loopval = None

    runloop = cli.RunLoop()
    with pytest.raises(StopTest):
        runloop.init(config, args, state)

    assert loopval == 42


@pytest.mark.skipif(sys.platform == 'win32', reason='windows')
def test_use_uvloop(monkeypatch):
    """Test that uvloop is set as default loop"""
    import uvloop

    class StopTest(Exception):
        pass

    monkeypatch.setattr(loadlimit.importhook, 'lstaskfiles', fake_lstaskfiles)
    monkeypatch.setattr(loadlimit.importhook, 'SourceFileLoader',
                        FakeSourceFileLoader)

    def fake_eventlooppolicy():
        return 42

    def fake_set_event_loop_policy(l):
        nonlocal loopval
        loopval = l
        raise StopTest

    def fake_processoptions(c, s):
        raise RuntimeError

    monkeypatch.setattr(uvloop, 'EventLoopPolicy', fake_eventlooppolicy)
    monkeypatch.setattr(asyncio, 'set_event_loop_policy', fake_set_event_loop_policy)
    monkeypatch.setattr(cli, 'process_options', fake_processoptions)

    taskfile = 'a_0.py'
    config = defaultdict(dict)
    state = Namespace()
    args = [cli.PROGNAME, '-d', '1s', '--no-progressbar', taskfile]
    loopval = None

    runloop = cli.RunLoop()
    with pytest.raises(StopTest):
        runloop.init(config, args, state)

    assert loopval == 42


@pytest.mark.skipif(sys.platform == 'win32', reason='windows')
def test_uvloop_imported():
    """Test that uvloop is imported"""
    import uvloop
    assert hasattr(cli, 'uvloop')
    assert cli.uvloop is uvloop


def test_uvloop_notimported(monkeypatch):
    """Test that uvloop is not imported on win32 platform"""
    if 'loadlimit.cli' in sys.modules:
        del sys.modules['loadlimit.cli']

    if sys.platform != 'win32':
        monkeypatch.setattr(sys, 'platform', 'win32')

    import loadlimit.cli as cli
    assert not hasattr(cli, 'uvloop')


# ============================================================================
#
# ============================================================================
