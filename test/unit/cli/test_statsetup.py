# -*- coding: utf-8 -*-
# test/unit/cli/test_statsetup.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test StatSetup"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from contextlib import contextmanager, ExitStack
from pathlib import Path, PurePosixPath

# Third-party imports
from pandas import to_timedelta
import pytest
from sqlalchemy import create_engine

# Local imports
import loadlimit.cli as cli
from loadlimit.cli import StatSetup
import loadlimit.channel as channel
from loadlimit.core import BaseLoop
from loadlimit.util import aiter, Namespace
import loadlimit.stat as stat
from loadlimit.stat import CountStore, Result


# ============================================================================
# Fixtures
# ============================================================================


pytestmark = pytest.mark.usefixtures('fake_shutdown_channel',
                                     'fake_timedata_channel')


# ============================================================================
# Tests
# ============================================================================


def fake_create_engine(connstr):
    """Mock create_engine"""
    return create_engine('sqlite://')


def fake_pathjoin(*args):
    """docstring for fake_pathjoin"""
    return str(PurePosixPath(*args))


def test_statsetup_context(monkeypatch):
    """StatSetup context"""
    monkeypatch.setattr(cli, 'pathjoin', fake_pathjoin)
    monkeypatch.setattr(cli, 'create_engine', fake_create_engine)

    llconfig = dict(
        cache=dict(type='sqlite'),
        tempdir='/not/exist',
        periods=8
    )
    config = dict(loadlimit=llconfig)
    state = Namespace(write=cli.Printer())

    with StatSetup(config, state) as statsetup:
        pass

    assert statsetup.results is None


@pytest.mark.parametrize('numiter,xv', [
    (i, xv) for i in [1, 10, 1000]
    for xv in [None, 'sqlite']
])
def test_statsetup_results(monkeypatch, testloop, numiter, xv):
    """StatSetup results"""
    measure = CountStore()
    tempdir = '/not/exist'

    monkeypatch.setattr(cli, 'measure', measure)
    monkeypatch.setattr(cli, 'pathjoin', fake_pathjoin)
    for m in [cli, stat]:
        monkeypatch.setattr(m, 'create_engine', fake_create_engine)

    llconfig = dict(
        cache=dict(type='sqlite'),
        tempdir=tempdir,
        periods=1,
        export=dict(type=xv, targetdir=tempdir),
        qmaxsize=1000,
        flushwait=to_timedelta(0, unit='s')
    )
    config = dict(loadlimit=llconfig)
    state = Namespace(write=cli.Printer(), progressbar={})

    # Create coro to time
    @measure(name='churn')
    async def churn(i):
        """Do nothing"""
        await asyncio.sleep(0)

    # Create second coro to time
    @measure(name='churn_two')
    async def churn2(i):
        """Do nothing"""
        await asyncio.sleep(0)

    async def run():
        """run"""
        async for i in aiter(range(numiter)):
            await churn(i)
            await churn2(i)
        await channel.shutdown.send(0)

    statsetup = StatSetup(config, state)

    # Run all the tasks
    with ExitStack() as stack:
        # Enter contexts
        stack.enter_context(statsetup)
        main = stack.enter_context(BaseLoop())

        statsetup.startevent()

        # Start the loop
        asyncio.ensure_future(run())
        main.start()

    results = statsetup.results
    assert results is not None
    assert len(results) == 5
    total, response, rate, error, failure = results
    assert total is not None
    assert response is not None
    assert rate is not None
    assert error is None
    assert failure is None


# ============================================================================
# Export tests
# ============================================================================


class FakeDataFrame:

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        if 'index_label' in kwargs:
            self.index = Namespace(names=kwargs['index_label'])

    def to_csv(self, *args, **kwargs):
        """Simulate sending to csv"""
        assert self._args == args
        assert self._kwargs == kwargs

    def to_sql(self, *args, **kwargs):
        """Simulate sending to csv"""
        assert self._args == args
        assert self._kwargs == kwargs


def fake_mktime(val):
    """Fake time.mktime()"""
    return 42


class CreateEngine:

    def __call__(self, val):
        return self

    @contextmanager
    def begin(self):
        """Fake sqlengine.begin()"""
        yield self


def test_exportdf_csv(monkeypatch):
    """Calls DataFrame.to_csv() with correct args"""
    monkeypatch.setattr(stat, 'mktime', fake_mktime)

    export_dir = Path('/not', 'exist')
    filename = 'export_42.csv'
    expected_path = str(export_dir / filename)
    df = FakeDataFrame(expected_path, index_label='Name')

    r = Result()
    r.exportdf(df, 'export', 'csv', str(export_dir))


def test_exportdf_sqlite(monkeypatch):
    """Calls DataFrame.to_sql() with correct args"""

    fake_engine = CreateEngine()

    monkeypatch.setattr(stat, 'create_engine', fake_engine)
    monkeypatch.setattr(stat, 'mktime', fake_mktime)

    export_dir = Path('/not', 'exist')
    df = FakeDataFrame('total', fake_engine)

    r = Result()
    r.exportdf(df, 'export', 'sqlite', str(export_dir))


# ============================================================================
#
# ============================================================================
