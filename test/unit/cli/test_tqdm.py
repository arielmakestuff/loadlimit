# -*- coding: utf-8 -*-
# test/unit/core/test_tqdm.py
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
import loadlimit.core as core
import loadlimit.event as event
import loadlimit.importhook
from loadlimit.importhook import mkmodule
from loadlimit.util import Namespace


# ============================================================================
# Fixtures
# ============================================================================


@pytest.yield_fixture
def pbar():
    """Create a tqdm progress bar"""
    config = dict(loadlimit={'show-progressbar': True})
    state = None
    with cli.tqdm_context(config, state=state) as pbar:
        yield pbar


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

    async def __call__(self, state):
        state.value += 1

    async def init(self, config, state):
        state.value = 0
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


def test_tqdm(monkeypatch, modpath):
    """Enable tqdm progress bars"""
    # Setup fake shutdown
    fake_shutdown = event.RunLast()
    fake_shutdown(core.shutdown, runlast=True)

    monkeypatch.setattr(event, 'shutdown', fake_shutdown)
    monkeypatch.setattr(loadlimit.importhook, 'lstaskfiles', fake_lstaskfiles)
    monkeypatch.setattr(loadlimit.importhook, 'SourceFileLoader',
                        FakeSourceFileLoader)

    taskfile = 'a_0.py'
    monkeypatch.setattr(sys, 'argv', [cli.PROGNAME, '-d', '2s', taskfile])

    config = defaultdict(dict)
    state = Namespace()
    with pytest.raises(SystemExit) as err:
        main(config=config, state=state)

    assert err.value.args == (0, )

    assert state.value > 0
    print(state.value)


def test_tqdm_reschedule(pbar, testloop):
    """Setting reschedule to False stops update_tqdm() coro"""
    name = 'test'
    state = Namespace(tqdm_progress={name: 0}, progressbar={name: pbar},
                      reschedule=True)

    async def stoptqdm(state):
        """stoptqdm"""
        await asyncio.sleep(1)
        state.reschedule = False

    tasks = [stoptqdm(state), cli.update_tqdm(None, state, name)]
    f = asyncio.gather(*tasks)
    testloop.run_until_complete(f)


def test_tqdm_stop(pbar, testloop):
    """stop_tqdm does not update pbar if pbar.total is None"""
    name = 'test'
    state = Namespace(tqdm_progress={name: 42}, progressbar={name: pbar})
    assert pbar.total is None

    f = asyncio.gather(cli.stop_tqdm(None, state=state, name=name))
    testloop.run_until_complete(f)

    # Progress was not changed
    assert state.tqdm_progress[name] == 42


def test_tqdm_client(pbar, testloop):
    """TQDMClient.__call__() coro keeps running until reschedule is False"""
    name = 'iteration'
    state = Namespace(tqdm_progress={name: 0}, progressbar={name: pbar})

    async def noop():
        """Do nothing"""
        client.option.reschedule = False

    client = cli.TQDMClient(core.Task(noop), reschedule=True)
    f = asyncio.gather(client(state))
    testloop.run_until_complete(f)

    assert state.tqdm_progress[name] == 1


def test_tqdm_noupdate(pbar, testloop):
    """tqdm pbar is not updated is its total has been reached"""
    pbar.total = 1
    name = 'test'
    state = Namespace(tqdm_progress={name: 0}, progressbar={name: pbar},
                      reschedule=True)

    async def stoptqdm(state):
        """stoptqdm"""
        await asyncio.sleep(1)
        state.reschedule = False

    tasks = [stoptqdm(state), cli.update_tqdm(None, state, name)]
    f = asyncio.gather(*tasks)
    testloop.run_until_complete(f)

    assert state.tqdm_progress[name] == 0


def test_tqdm_nopbar(testloop):
    """Use parent class __call__ if no tqdm pbar"""
    state = Namespace(progressbar={}, value=0)

    class Task(core.TaskABC):
        """Custom task"""

        async def __call__(self, state):
            state.value += 1

        async def init(self, config, state):
            """noop"""

    client = cli.TQDMClient(Task)
    f = asyncio.gather(client(state))
    testloop.run_until_complete(f)


# ============================================================================
#
# ============================================================================
