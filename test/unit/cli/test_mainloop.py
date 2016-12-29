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
    state = Namespace()
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
#
# ============================================================================
