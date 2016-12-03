# -*- coding: utf-8 -*-
# test/unit/util/test_logger.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test Logger"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports

# Third-party imports
import pytest

# Local imports
from loadlimit.util import Logger


# ============================================================================
# Tests
# ============================================================================


def test_getattr_unknown_name(caplog):
    """Raise error if trying to get an invalid log func"""
    expected = 'Unknown log function name: {}'.format('hello')

    l = Logger(logger=caplog)
    with pytest.raises(ValueError) as err:
        getattr(l, 'hello')

    assert err.value.args == (expected, )


@pytest.mark.parametrize('level', [42, 4.2, '42', [42]])
def test_log_badlevel(caplog, level):
    """Raise error if giving an invalid value for level arg"""
    expected = ('level expected LogLevel, got {} instead'.
                format(type(level).__name__))

    l = Logger(logger=caplog)
    with pytest.raises(TypeError) as err:
        l.log(level, 'hello')

    assert err.value.args == (expected, )

# ============================================================================
#
# ============================================================================
