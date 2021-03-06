# -*- coding: utf-8 -*-
# test/unit/core/test_baseloop.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test loadlimit.core.BaseLoop"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
import logging
import os
from signal import SIGTERM, SIGINT
import sys

# Third-party imports
import pytest

# Local imports
import loadlimit.core as core
from loadlimit.core import BaseLoop
from loadlimit.util import LogLevel, Namespace
import loadlimit.channel as channel


# ============================================================================
# Globals
# ============================================================================


pytestmark = pytest.mark.usefixtures('testlogging', 'fake_shutdown_channel')


# ============================================================================
# Test __init__
# ============================================================================


def test_init():
    """__init__() sets default values for private attrs"""
    baseloop = BaseLoop()
    assert baseloop._loop is not None
    assert isinstance(baseloop._loop, asyncio.AbstractEventLoop)
    assert baseloop._loopend is None
    assert baseloop._logoptions == {'init-logging': True, 'level':
                                    LogLevel.INFO, 'name': 'loadlimit'}


def test_init_non_looptype():
    """Giving a non-AbstractEventLoop type is an error"""
    expected = 'loop expected AbstractEventLoop, got int instead'
    with pytest.raises(TypeError) as err:
        BaseLoop(loop=42)

    assert err.value.args == (expected, )


# ============================================================================
# Test loglevel
# ============================================================================


def test_loglevel():
    """loglevel is set to the value of the loglevel arg"""
    for loglevel in LogLevel:
        baseloop = BaseLoop(loglevel=loglevel)
        assert baseloop.loglevel == loglevel


def test_loglevel_noninit(monkeypatch):
    """Setting loglevel outside of init triggers re-init of logging"""

    called = False

    def fake_initlogging(self):
        """fake_initlogging"""
        nonlocal called
        called = True

    monkeypatch.setattr(BaseLoop, 'initlogging', fake_initlogging)
    b = BaseLoop()
    b.loglevel = LogLevel.WARNING
    assert called is True


def test_loglevel_badvalue():
    """Non-LogLevel value raises TypeError"""
    expected = 'loglevel expected LogLevel, got str'
    with pytest.raises(TypeError) as err:
        BaseLoop(loglevel='bad value')
    assert err.value.args == (expected, )


def test_loglevel_logging_value():
    """Valid value from logging module still raises error"""
    loglevelgen = (loglevel for n, loglevel in logging._nameToLevel.items()
                   if n not in ['WARN', 'NOTSET'])
    for loglevel in loglevelgen:
        expected = 'loglevel expected LogLevel, got int'
        with pytest.raises(TypeError) as err:
            BaseLoop(loglevel=loglevel)
        assert err.value.args == (expected, )


def test_loglevel_logmsg(caplog):
    """log messages are shown in the appropriate log level"""
    expected = 'called warning'
    with caplog.at_level(logging.WARNING):
        baseloop = BaseLoop(loglevel=LogLevel.WARNING)
        logger = baseloop.logger
        logger.warning('called warning')
        logger.info('called info')
        assert len(caplog.records) == 1
        assert caplog.records[0].message == expected


@pytest.mark.parametrize('expected', [
    (LogLevel.WARNING, 'called warning', 1),
    (LogLevel.INFO, 'called info', 2)
])
def test_loglevel_switch(caplog, expected):
    """Messages logged in correct level after switching loglevel"""
    level, msg, nummsg = expected
    baseloop = BaseLoop(loglevel=level)
    baseloop.initlogging()
    logger = baseloop.logger
    with caplog.at_level(level.value):
        logger.info('called info')
        logger.warning('called warning')
        records = [r for r in caplog.records if r.name != 'asyncio']
        assert len(records) == nummsg
        assert records[0].message == msg


# ============================================================================
# Test SIGTERM and SIGINT signals
# ============================================================================


def choose_platform(platlist):
    """docstring for choose_platform"""
    curplatform = sys.platform
    if curplatform == 'win32':
        ret = [p for p in platlist if p == curplatform]
    else:
        ret = platlist
    return ret


@pytest.mark.skipif(sys.platform == 'win32', reason='windows')
@pytest.mark.parametrize('signal,platform', [
    (s, p) for s in [SIGTERM, SIGINT]
    for p in choose_platform(['win32', 'linux'])
])
def test_stopsignals(monkeypatch, caplog, testloop, signal, platform):
    """SIGTERM and SIGINT stop the loop"""
    if sys.platform != platform:
        monkeypatch.setattr(sys, 'platform', platform)

    async def killme(logger, signal):
        """Send signal to me"""
        logger.info('sending signal {}'.format(signal))
        os.kill(os.getpid(), signal)

    with BaseLoop() as loop:
        logger = loop.logger
        logger.info('Using signal {}'.format(signal))
        asyncio.ensure_future(killme(logger, signal))
        loop.start()

    expected = [
        'Using signal {}'.format(signal),
        'loop started',
        'sending signal {}'.format(signal),
        'got signal {} ({})'.format(signal.name, signal),
        'shutdown',
        'stopping loop',
        'cancelling tasks',
        'tasks cancelled',
        'loop closed'
    ]

    # Insert needed extra messages if on windows
    # if platform == 'win32':
    #     expected[-1:-1] = ['cancelling tasks', 'tasks cancelled']

    result = [r.message for r in caplog.records]
    assert result == expected


# ============================================================================
# Test uncaught exception handling
# ============================================================================


@pytest.mark.parametrize('exctype', [
    Exception, RuntimeError, ValueError
])
def test_stoperror(caplog, testloop, exctype):
    """Uncaught exceptions logs the exception type and message"""

    async def errorme(logger):
        """Raise exception"""
        logger.info('exception me!')
        raise exctype('what')

    with BaseLoop() as loop:
        logger = loop.logger
        asyncio.ensure_future(errorme(logger))
        loop.start()

    expected = [
        'loop started',
        'exception me!',
        'exception ({}): what'.format(exctype.__name__),
        'shutdown',
        'stopping loop',
        'cancelling tasks',
        'tasks cancelled',
        'loop closed'
    ]

    # Insert needed extra messages if on windows
    # if sys.platform == 'win32':
    #     expected[-1:-1] = ['cancelling tasks', 'tasks cancelled']

    result = [r.message for r in caplog.records]
    assert result == expected


def test_uncaught_exceptions_no_exception(monkeypatch, caplog):
    """Exception handler given context without an exception key in context"""

    calledput = False

    def fake_put(data):
        nonlocal calledput
        calledput = True

    monkeypatch.setattr(channel.shutdown, 'put', fake_put)
    monkeypatch.setattr(channel.shutdown, '_state',
                        channel.ChannelState.listening)

    msg = 'what'
    context = dict(message=msg)
    expected = 'got exception: {}'.format(msg)

    main = BaseLoop()
    main.uncaught_exceptions(main.loop, context)

    records = [r for r in caplog.records if r.name != 'asyncio']
    assert len(records) == 1
    assert records[0].message == expected
    assert calledput is True


@pytest.mark.parametrize('state', [
    s for s in channel.ChannelState if s != channel.ChannelState.listening
])
def test_uncaught_exceptions_shutdown_not_listening(monkeypatch, state):
    """Data is not put into shutdown channel if channel is not listening"""

    calledput = False

    def fake_put(data):
        nonlocal calledput
        calledput = True

    monkeypatch.setattr(channel.shutdown, 'put', fake_put)
    monkeypatch.setattr(channel.shutdown, '_state', state)

    msg = 'what'
    context = dict(message=msg)

    main = BaseLoop()
    main.uncaught_exceptions(main.loop, context)

    assert calledput is False


def test_uncaught_exceptions_shutdown_listening(monkeypatch):
    """Data is put into shutdown channel if channel is listening"""

    calledput = False

    def fake_put(data):
        nonlocal calledput
        calledput = True

    monkeypatch.setattr(channel.shutdown, 'put', fake_put)
    monkeypatch.setattr(channel.shutdown, '_state',
                        channel.ChannelState.listening)

    msg = 'what'
    context = dict(message=msg)

    main = BaseLoop()
    main.uncaught_exceptions(main.loop, context)

    assert calledput is True


# ============================================================================
# Test run
# ============================================================================


def test_run_stoperror(caplog, testloop):
    """run -- uncaught exceptions shuts the loop down"""
    async def errorme(logger):
        """Raise exception"""
        logger.info('exception me!')
        raise Exception('what')

    loop = BaseLoop()
    tasks = [errorme(loop.logger)]
    exitcode = loop.run(tasks)

    # Uncaught error happened
    assert exitcode == 1

    expected = [
        'loop started',
        'exception me!',
        'exception (Exception): what',
        'shutdown',
        'stopping loop',
        'cancelling tasks',
        'tasks cancelled',
        'loop closed'
    ]

    # Insert needed extra messages if on windows
    # if sys.platform == 'win32':
    #     expected[-1:-1] = ['cancelling tasks', 'tasks cancelled']

    result = [r.message for r in caplog.records]
    assert result == expected


@pytest.mark.skipif(sys.platform == 'win32', reason='windows')
@pytest.mark.parametrize('signal,platform', [
    (s, p) for s in [SIGTERM, SIGINT]
    for p in choose_platform(['win32', 'linux'])
])
def test_run_stopsignals(monkeypatch, caplog, testloop, signal, platform):
    """run -- SIGTERM and SIGINT stop the loop"""
    if sys.platform != platform:
        monkeypatch.setattr(sys, 'platform', platform)

    async def killme(logger, signal):
        """Send signal to me"""
        logger.info('sending signal {}'.format(signal))
        os.kill(os.getpid(), signal)

    loop = BaseLoop()
    logger = loop.logger
    logger.info('Using signal {}'.format(signal))
    exitcode = loop.run([killme(logger, signal)])
    assert exitcode == 0

    expected = [
        'Using signal {}'.format(signal),
        'loop started',
        'sending signal {}'.format(signal),
        'got signal {} ({})'.format(signal.name, signal),
        'shutdown',
        'stopping loop',
        'cancelling tasks',
        'tasks cancelled',
        'loop closed'
    ]

    # Insert needed extra messages if on windows
    # if platform == 'win32':
    #     expected[-1:-1] = ['cancelling tasks', 'tasks cancelled']

    result = [r.message for r in caplog.records]
    assert result == expected


# ============================================================================
# Test running
# ============================================================================


def test_running(testloop):
    """running attr returns True if loop is still running"""

    async def check_loop_state(baseloop, future):
        """Check if baseloop is still running"""
        future.set_result(baseloop.running())
        result = Namespace(exitcode=0)
        asyncio.ensure_future(core.shutdown(result, manager=baseloop))

    with BaseLoop() as baseloop:
        future = asyncio.get_event_loop().create_future()
        asyncio.ensure_future(check_loop_state(baseloop, future))
        baseloop.start()

    assert future.result() is True
    assert baseloop.running() is False


# ============================================================================
# Test exitcode
# ============================================================================


def test_exitcode_loop_not_started():
    """exitcode returns None if loop has not started"""
    baseloop = BaseLoop()
    assert baseloop.exitcode is None


def test_exitcode_loop_still_running(testloop):
    """exitcode returns None if loop is still running"""

    async def check_exitcode(baseloop, future):
        """Check exitcode"""
        future.set_result(baseloop.exitcode)
        await channel.shutdown.send(42)

    with BaseLoop() as loop:
        future = asyncio.get_event_loop().create_future()
        asyncio.ensure_future(check_exitcode(loop, future))
        loop.start()

    assert future.result() is None
    assert loop.exitcode == 42


# ============================================================================
# Test loop property
# ============================================================================


def test_loop_defaultval():
    """Returns the default asyncio loop"""
    b = BaseLoop()
    assert b.loop is b._loop


def test_loop_custom(testloop):
    """Returns the same asyncio loop given via kwargs"""
    b = BaseLoop(loop=testloop)
    assert b.loop is testloop


# ============================================================================
# Test shutting down multiple times
# ============================================================================


def test_multiple_shutdown(caplog, testloop):
    """Main loop shutdown can be invoked multiple times"""

    async def multi_shutdown(baseloop):
        """Check if baseloop is still running"""
        result = Namespace(exitcode=0)
        for i in range(2):
            asyncio.ensure_future(core.shutdown(result, manager=baseloop))

    level = LogLevel.INFO
    with caplog.at_level(level.value):
        with BaseLoop(loglevel=level) as baseloop:
            asyncio.ensure_future(multi_shutdown(baseloop))
            baseloop.start()

        records = [r for r in caplog.records if r.name != 'asyncio'
                   and r.message == 'shutdown']
        assert len(records) == 2


# ============================================================================
#
# ============================================================================
