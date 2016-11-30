# -*- coding: utf-8 -*-
# loadlimit/test/unit/conftest.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Pytest config for unit tests"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from contextlib import contextmanager
import logging

# Third-party imports
import pytest

# Local imports
import loadlimit.cli as cli
import loadlimit.core as core
import loadlimit.event as event
import loadlimit.stat as stat


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def datadir():
    """Return the full datadir path"""
    rootdir = pytest.config.rootdir
    return rootdir / 'test' / 'data'


@pytest.fixture
def testlogging(caplog):
    """Initializes log level for the test"""
    caplog.set_level(logging.INFO)


# ============================================================================
# Event loop fixtures
# ============================================================================


@pytest.yield_fixture
def testloop(event_loop):
    """Cleanup event_loop run"""
    asyncio.set_event_loop(event_loop)
    yield event_loop
    f = asyncio.gather(*asyncio.Task.all_tasks(loop=event_loop),
                       loop=event_loop)
    f.cancel()
    try:
        event_loop.run_until_complete(f)
    except asyncio.CancelledError:
        pass
    finally:
        event_loop.close()


@pytest.fixture
def fake_shutdown_event(monkeypatch):
    """Setup fake shutdown event"""
    fake_shutdown = event.RunLast()
    fake_shutdown(core.shutdown, runlast=True)
    monkeypatch.setattr(event, 'shutdown', fake_shutdown)


@pytest.fixture
def fake_recordperiod_event(monkeypatch):
    """Setup fake recordperiod event"""
    fake_recordperiod = event.MultiEvent(event.RunFirst)
    fake_recordperiod(stat.updateperiod, runfirst=True)
    monkeypatch.setattr(stat, 'recordperiod', fake_recordperiod)


@pytest.fixture
def fake_tempdir(monkeypatch):
    """Mock loadlimit.cli.TemporaryDirectory"""

    @contextmanager
    def fake_tempdir():
        yield '/fake/path'

    monkeypatch.setattr(cli, 'TemporaryDirectory', fake_tempdir)


# ============================================================================
#
# ============================================================================
