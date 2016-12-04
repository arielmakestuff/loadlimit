# -*- coding: utf-8 -*-
# test/unit/cli/test_printer.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test Printer"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports

# Third-party imports
import pytest

# Local imports
from loadlimit.cli import Printer


# ============================================================================
# Tests
# ============================================================================


def test_custom_writefunc():
    """Printer formats messages like print() for non-print functions"""
    val = []
    expected = ['START', 'MIDDLE END\n']

    def myprint(msg):
        val.extend(['START', msg])

    p = Printer(myprint)
    p('MIDDLE', 'END')

    assert val == expected


@pytest.mark.parametrize('func', [42, 4.2, '42', [42]])
def test_noncallable(func):
    """Raise error if printfunc arg is given a non-callable"""
    expected = ('printfunc expected callable, got value of type {}'.
                format(type(func).__name__))
    with pytest.raises(TypeError) as err:
        Printer(func)

    assert err.value.args == (expected, )


@pytest.mark.parametrize('val', [42, 4.2])
def test_nonprint_nonstrvalue(val):
    """Accept any non-str values for non-print functions"""
    msg = None

    def myprint(value):
        nonlocal msg
        msg = value

    p = Printer(myprint)
    p(val)

    assert msg == '{}\n'.format(val)


# ============================================================================
#
# ============================================================================
