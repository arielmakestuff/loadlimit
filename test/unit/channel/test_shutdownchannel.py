# -*- coding: utf-8 -*-
# test/unit/channel/test_shutdownchannel.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test ShutdownChannel"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import logging

# Third-party imports
import pytest

# Local imports
import loadlimit.channel as channel
from loadlimit.channel import ShutdownChannel


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def testlogging(caplog):
    """Initializes log level for the test"""
    caplog.set_level(logging.WARNING)


# ============================================================================
# Globals
# ============================================================================


pytestmark = pytest.mark.usefixtures('testlogging')


# ============================================================================
# Test put data (full channel)
# ============================================================================


@pytest.mark.parametrize('val', [42, 4.2, '42'])
def test_full(caplog, val):
    """Issue a warning and don't put if channel already contains data"""

    msg = ('Not sending shutdown({}): shutdown already requested'.
           format(val))
    expected = ['channel shutdown: open', msg]

    shutdown = channel.ShutdownChannel()

    with shutdown.open():
        # Put data in the channel before the test.
        shutdown.put(1)

        # Can new data be put?
        shutdown.put(val)

    records = [r for r in caplog.records if r.name != 'asyncio']
    assert len(records) == 2
    assert [r.message for r in records] == expected


# ============================================================================
#
# ============================================================================
