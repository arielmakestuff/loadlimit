# -*- coding: utf-8 -*-
# test/unit/cli/test_mainloop.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test main loop"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
import logging

# Third-party imports
from pandas import to_timedelta
import pytest

# Local imports
from loadlimit.cli import MainLoop
from loadlimit.util import Namespace, TZ_UTC


# ============================================================================
# Tests
# ============================================================================


def test_initlogformatter_filehandler():
    """Set a logging.FileHandler as the only handler"""
    main = MainLoop()
    main.logoptions.update(logfile='hello.log', tz=TZ_UTC)
    f = main.initlogformatter()
    hlist = main.initloghandlers(f)
    assert len(hlist) == 1
    h = hlist[0]
    assert isinstance(h, logging.FileHandler)


# ============================================================================
# Test initclients()
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize('initrate,numclients', [
    (initrate, numclients) for initrate in [1, 10, 0]
    for numclients in [10, 1]
])
async def test_initclients_initialize(monkeypatch, initrate, numclients):
    """Initialize clients according to rate"""
    sleepvals = set()

    # Setup the test
    class TestClient:

        def __init__(self):
            self.initialized = False

        async def init(self, c, s):
            self.initialized = True

    async def fake_sleep(s):
        sleepvals.add(s)
        return

    monkeypatch.setattr(asyncio, 'sleep', fake_sleep)

    clients = [TestClient() for i in range(numclients)]
    config = dict(loadlimit=dict(initrate=initrate))
    state = Namespace(event=[])
    loop = asyncio.get_event_loop()

    # Run initclients()
    mainloop = MainLoop()
    await mainloop.initclients(config, state, clients, loop)

    # Check everything was initialized
    # initrate == 0 is the same as having initrate == numclients
    if initrate and numclients > initrate:
        assert len(sleepvals) == 1
        assert 1 in sleepvals
    assert all(c.initialized for c in clients)


@pytest.mark.asyncio
async def test_initclients_noclients():
    """Do nothing if given no clients"""
    mainloop = MainLoop()
    await mainloop.initclients(None, None, [], None)


# ============================================================================
# Test schedclients()
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize('size,delay,numclients', [
    (size, delay, numclients)
    for size in [0, 5, 10]
    for delay in [1, 10]
    for numclients in [10]
])
async def test_schedclients_schedule(monkeypatch, size, delay, numclients):
    """Schedule clients according to rate"""
    loop = asyncio.get_event_loop()
    sleepvals = []

    # Setup the test
    class TestClient:
        queue = asyncio.Queue(maxsize=numclients, loop=loop)

        def __init__(self):
            self.called = False
            self.queue.put_nowait(True)

        async def __call__(self, state):
            self.called = True
            self.queue.get_nowait()
            self.queue.task_done()

    async def fake_sleep(s):
        sleepvals.append(s)
        return

    monkeypatch.setattr(asyncio, 'sleep', fake_sleep)

    clients = [TestClient() for i in range(numclients)]
    config = dict(loadlimit=dict(schedsize=size,
                                 sched_delay=to_timedelta(delay, unit='s')))
    state = Namespace(reschedule=True, event=[])

    # Run schedclients()
    mainloop = MainLoop()
    await mainloop.schedclients(config, state, clients, loop)
    await TestClient.queue.join()

    # Check everything was scheduled
    # schedsize == 0 is the same as having schedsize == numclients
    if size > 0 and size < numclients:
        numiter = numclients // size
        assert len(sleepvals) == numiter
        assert sleepvals == [delay] * numiter
    else:
        assert len(sleepvals) == 1
        assert sleepvals == [0]
    assert all(c.called for c in clients)


@pytest.mark.asyncio
async def test_schedclients_noclients():
    """Do nothing if given no clients"""
    mainloop = MainLoop()
    await mainloop.schedclients(None, None, [], None)


@pytest.mark.asyncio
async def test_schedclients_noreschedule(monkeypatch):
    """Schedule clients according to rate"""
    # 2 batches to process all clients
    size = 5
    delay = 10
    numclients = 10

    # Setup loop
    loop = asyncio.get_event_loop()
    sleepvals = []

    # Setup the test
    class TestClient:
        queue = asyncio.Queue(maxsize=numclients, loop=loop)

        def __init__(self):
            self.called = False
            self.queue.put_nowait(True)

        async def __call__(self, state):
            self.called = True
            self.queue.get_nowait()
            self.queue.task_done()

            # After first batch of clients called, don't call another batch
            state.reschedule = False

    async def fake_sleep(s):
        # Remove from the queue all clients except for the first batch
        for i in range(numclients-size):
            TestClient.queue.get_nowait()
            TestClient.queue.task_done()
        sleepvals.append(s)

        # Wait until the first batch of clients completes
        await TestClient.queue.join()

    monkeypatch.setattr(asyncio, 'sleep', fake_sleep)

    clients = [TestClient() for i in range(numclients)]
    config = dict(loadlimit=dict(schedsize=size,
                                 sched_delay=to_timedelta(delay, unit='s')))
    state = Namespace(reschedule=True, event=[])

    # Run schedclients()
    mainloop = MainLoop()
    await mainloop.schedclients(config, state, clients, loop)

    # Check only a single iteration was scheduled and only the first batch of
    # clients was called
    assert len(sleepvals) == 1
    assert sleepvals == [delay]
    numcalled = sum(1 for c in clients if c.called)
    numnotcalled = sum(1 for c in clients if not c.called)
    assert numcalled == size
    assert numnotcalled == numclients - size


# ============================================================================
#
# ============================================================================
