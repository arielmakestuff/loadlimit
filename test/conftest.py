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
import logging

# Third-party imports
import pytest

# Local imports


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


# ============================================================================
#
# ============================================================================
