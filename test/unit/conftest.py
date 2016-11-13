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
import logging

# Third-party imports
import pytest

# Local imports


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def testlogging(caplog):
    """Initializes log level for the test"""
    caplog.set_level(logging.INFO)


# ============================================================================
#
# ============================================================================
