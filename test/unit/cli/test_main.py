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
import os
from pathlib import Path
import sys

# Third-party imports
from pandas import Timedelta
import pytest
from pytz import timezone

# Local imports
import loadlimit.cli as cli
from loadlimit.cli import main, PROGNAME
from loadlimit.importhook import TaskImporter
from loadlimit.util import LogLevel


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
    def fake_runloop(self, config, args, state):
        """fake_runloop"""
        cli.process_options(config, args)

    monkeypatch.setattr(cli.RunLoop, '__call__', fake_runloop)


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

    names = ['timezone', 'numusers', 'duration', 'importer',
             'show-progressbar', 'cache', 'export', 'periods',
             'logging', 'qmaxsize', 'flushwait', 'initrate']
    assert len(llconfig) == len(names)
    for name in names:
        assert name in llconfig

    assert llconfig['numusers'] == 1
    assert llconfig['timezone'] == timezone('UTC')
    assert llconfig['duration'] == Timedelta('1s')
    assert llconfig['show-progressbar'] is True
    assert llconfig['cache']['type'] == 'memory'
    assert llconfig['export']['type'] is None
    assert 'targetdir' not in llconfig['export']
    assert isinstance(llconfig['importer'], TaskImporter)
    assert llconfig['periods'] == 8
    assert llconfig['logging']['loglevel'] == LogLevel.WARNING
    assert llconfig['qmaxsize'] == 1000
    assert llconfig['flushwait'] == Timedelta('2s')
    assert llconfig['initrate'] == 0


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


@pytest.mark.parametrize('val', [0, 1])
def test_main_periods_badvalue(val):
    """Raise error if periods is given value <= 1"""
    config = {}
    args = ['-p', str(val), '-d', '1s', 'what']

    expected = 'periods option must be > 1'
    with pytest.raises(ValueError) as err:
        main(arglist=args, config=config)

    assert err.value.args == (expected, )


def test_main_export_baddir(monkeypatch):
    """Raise error if directory does not exist"""

    def fake_isdir(n):
        """fake_isdir"""
        return False

    monkeypatch.setattr(cli, 'isdir', fake_isdir)
    config = {}
    args = ['-E', 'csv', '-e', '/not/exist', '-d', '1s', 'what']

    expected = '/not/exist'
    with pytest.raises(FileNotFoundError) as err:
        main(arglist=args, config=config)

    assert err.value.args == (expected, )


def test_main_export_targetdir(monkeypatch):
    """Store export directory in internal config"""

    def fake_isdir(n):
        """fake_isdir"""
        return True

    monkeypatch.setattr(cli, 'isdir', fake_isdir)
    config = {}
    args = ['-E', 'csv', '-e', '/not/exist', '-d', '1s', 'what']

    with pytest.raises(SystemExit):
        main(arglist=args, config=config)

    llconfig = config['loadlimit']
    assert 'export' in llconfig

    exportconfig = llconfig['export']
    assert exportconfig['type'] == 'csv'
    assert exportconfig['targetdir'] == '/not/exist'


def test_main_export_nodir(monkeypatch):
    """Use current directory if targetdir not given"""
    config = {}
    args = ['-E', 'csv', '-d', '1s', 'what']

    with pytest.raises(SystemExit):
        main(arglist=args, config=config)

    llconfig = config['loadlimit']
    assert 'export' in llconfig

    exportconfig = llconfig['export']
    assert exportconfig['type'] == 'csv'
    assert exportconfig['targetdir'] == os.getcwd()


def test_main_logfile_default():
    """Default logfile"""
    config = {}
    args = ['-L', '-d', '1s', 'what']

    with pytest.raises(SystemExit):
        main(arglist=args, config=config)

    llconfig = config['loadlimit']
    assert 'logging' in llconfig

    expected = Path.cwd() / '{}.log'.format(cli.PROGNAME)
    assert llconfig['logging']['logfile'] == str(expected)


def test_main_logfile_bad_parentdir(monkeypatch):
    """Raise error if given logfile path's parent doesn't exist"""

    filename = Path('/imaginary/path/notexist')

    def fake_isdir(self):
        """fake_isdir"""
        return False

    monkeypatch.setattr(cli.Path, 'is_dir', fake_isdir)

    config = {}
    args = ['-L', '-l', str(filename), '-d', '1s', 'what']

    with pytest.raises(FileNotFoundError) as err:
        main(arglist=args, config=config)

    assert err.value.args == (str(filename.parent), )


def test_main_logfile_isdir(monkeypatch):
    """Raise error if given logfile is a directory"""

    filename = Path('/imaginary/path/notexist')

    def fake_isdir(self):
        """fake_isdir"""
        return True

    monkeypatch.setattr(cli.Path, 'is_dir', fake_isdir)

    config = {}
    args = ['-L', '-l', str(filename), '-d', '1s', 'what']

    with pytest.raises(IsADirectoryError) as err:
        main(arglist=args, config=config)

    assert err.value.args == (str(filename), )


@pytest.mark.parametrize('val', ['hello', (42, )])
def test_main_flushwait_badval(val):
    """Raise error if flushwait is given bad value"""
    config = {}
    args = ['--flush-wait', str(val), '-d', '1s', 'what']

    expected = 'duration option got invalid value: {}'.format(val)
    with pytest.raises(ValueError) as err:
        main(arglist=args, config=config)

    assert err.value.args == (expected, )


# ============================================================================
#
# ============================================================================
