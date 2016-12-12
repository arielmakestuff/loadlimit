# -*- coding: utf-8 -*-
# loadlimit/test/unit/stat/conftest.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Pytest config for unit tests"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports

# Third-party imports
from pandas import Timestamp, to_timedelta
import pytest
from sqlalchemy import create_engine

# Local imports
import loadlimit.stat as stat


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sqlengine():
    """docstring for initsqlite"""
    engine = create_engine('sqlite://')
    return engine


@pytest.fixture
def statsdict():
    """Create an empty Period object"""
    statsdict = stat.Period()
    statsdict.end_date = end = Timestamp.now(tz='UTC')
    statsdict.start_date = end - to_timedelta(5, unit='s')
    return statsdict


# ============================================================================
#
# ============================================================================
