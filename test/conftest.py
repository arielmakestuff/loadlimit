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
import loadlimit.channel as channel
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
    if event_loop.is_closed():
        return
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
def fake_shutdown_channel(monkeypatch):
    """Setup fake shutdown event"""
    fake_shutdown = channel.ShutdownChannel()
    fake_shutdown(core.shutdown, anchortype=channel.AnchorType.last)
    monkeypatch.setattr(channel, 'shutdown', fake_shutdown)


@pytest.fixture
def fake_timedata_channel(monkeypatch):
    """Setup fake timedata channel"""
    fake_timedata = event.MultiEvent(event.RunFirst)
    fake_timedata = channel.DataChannel()
    fake_timedata(stat.updateperiod, anchortype=channel.AnchorType.first)
    monkeypatch.setattr(stat, 'timedata', fake_timedata)


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
