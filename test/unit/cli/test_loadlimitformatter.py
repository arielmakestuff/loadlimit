# -*- coding: utf-8 -*-
# test/unit/cli/test_loadlimitformatter.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test LoadLimitFormatter"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
import logging

# Third-party imports

# Local imports
from loadlimit.cli import LoadLimitFormatter, PROGNAME
import loadlimit.channel as channel
from loadlimit.core import BaseLoop


# ============================================================================
# Tests
# ============================================================================


def test_no_datefmt(testloop, caplog):
    """Use default format"""

    caplog.set_level(logging.WARNING)

    async def run(logger):
        """run"""
        logger.warning('hello world')
        await channel.shutdown.send(0)

    with BaseLoop() as main:
        logger = main.logger
        main.initlogging(fmtcls=LoadLimitFormatter,
                         style='{',
                         format='{asctime} {message}')
        asyncio.ensure_future(run(logger))
        main.start()

    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.name == PROGNAME
    assert record.levelname == 'WARNING'
    assert record.message.endswith('hello world')


# ============================================================================
#
# ============================================================================
