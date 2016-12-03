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
import logging

# Third-party imports

# Local imports
from loadlimit.cli import MainLoop
from loadlimit.util import TZ_UTC


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
#
# ============================================================================
