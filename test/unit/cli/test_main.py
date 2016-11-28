# -*- coding: utf-8 -*-
# test/unit/cli/test_main.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test main()"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import sys

# Third-party imports
from pandas import Timedelta
import pytest
from pytz import timezone

# Local imports
import loadlimit.cli as cli
from loadlimit.cli import main, PROGNAME
from loadlimit.importhook import TaskImporter


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def empty_argv(monkeypatch):
    """Set sys.argv to an empty list"""
    monkeypatch.setattr(sys, 'argv', [])


@pytest.fixture
def norunloop(monkeypatch):
    """Mock runloop() with func that does nothing"""
    def fake_runloop(config, args, state):
        """fake_runloop"""
        cli.process_options(config, args)

    monkeypatch.setattr(cli, 'runloop', fake_runloop)


pytestmark = pytest.mark.usefixtures('empty_argv', 'norunloop')


# ============================================================================
# Test main
# ============================================================================


def test_main_help(capsys):
    """main"""
    with pytest.raises(SystemExit) as err:
        main()

    assert err.value.args == (0, )

    # Check stdout
    out, err = capsys.readouterr()
    assert out.startswith('usage: {}'.format(PROGNAME))


def test_main_nonempty_sysargv(monkeypatch, capsys):
    """Non-empty sys.argv list"""

    monkeypatch.setattr(sys, 'argv', ['loadlimit', '-h'])
    with pytest.raises(SystemExit) as err:
        main()

    assert err.value.args == (0, )

    # Check stdout
    out, err = capsys.readouterr()
    assert out.startswith('usage: {}'.format(PROGNAME))


def test_main_loadlimit_configsection(capsys):
    """loadlimit config section exists in dict passed to main"""
    config = dict(loadlimit={})
    with pytest.raises(SystemExit) as err:
        main(config=config)

    assert err.value.args == (0, )

    # Check stdout
    out, err = capsys.readouterr()
    assert out.startswith('usage: {}'.format(PROGNAME))


def test_main_default_args():
    """Config default values"""
    config = {}
    args = ['-d', '1s', 'what']
    with pytest.raises(SystemExit):
        main(arglist=args, config=config)

    assert config
    assert len(config) == 1
    assert 'loadlimit' in config

    llconfig = config['loadlimit']

    assert len(llconfig) == 5
    for name in ['timezone', 'numusers', 'duration', 'importer',
                 'show-progressbar']:
        assert name in llconfig

    assert llconfig['numusers'] == 1
    assert llconfig['timezone'] == timezone('UTC')
    assert llconfig['duration'] == Timedelta('1s')
    assert llconfig['show-progressbar'] is True
    assert isinstance(llconfig['importer'], TaskImporter)


@pytest.mark.parametrize('val', ['fhjdsf', '42z', 'one zots'])
def test_main_bad_duration(val):
    """Invalid value for duration option raises an error"""
    config = {}
    args = ['-d', val, 'what']
    with pytest.raises(ValueError):
        main(arglist=args, config=config)


@pytest.mark.parametrize('val', [None, ''])
def test_main_empty_duration(val):
    """Not giving a duration raises an error"""
    config = {}
    args = ['what']
    if val is not None:
        args[:0] = ['-d', val]

    expected = 'duration option got invalid value {!r}'.format(val)
    with pytest.raises(ValueError) as err:
        main(arglist=args, config=config)

    assert err.value.args == (expected, )


def test_main_bad_users():
    """Value < 0 for users option raises error"""
    config = {}
    args = ['-u', '0', 'what']

    expected = 'users option expected value > 0, got 0'
    with pytest.raises(ValueError) as err:
        main(arglist=args, config=config)

    assert err.value.args == (expected, )


# ============================================================================
#
# ============================================================================
