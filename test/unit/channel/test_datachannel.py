# -*- coding: utf-8 -*-
# test/unit/channel/test_datachannel.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test DataChannel class"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from collections.abc import MutableMapping
from contextlib import ExitStack
from functools import partial
from random import choice

# Third-party imports
import pytest

# Local imports
from loadlimit.channel import (AnchorType, ChannelState, ChannelClosedError,
                               ChannelListeningError, ChannelOpenError,
                               DataChannel, NotListeningError)
from loadlimit.util import aiter, LogLevel, Namespace


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def testchannel():
    """Return DataChannel object"""
    return DataChannel()


# ============================================================================
# Test decorator
# ============================================================================


@pytest.mark.parametrize('keycls', [Namespace, dict])
def test_decorator_keyobj(testchannel, keycls):
    """Decorating a callable adds it as a channel task"""
    keyobj = keycls()

    @testchannel(keyobj=keyobj)
    async def one():
        """one"""

    getfunc = (keyobj.get if isinstance(keyobj, MutableMapping)
               else partial(getattr, keyobj))
    assert getfunc('key', None) == 0


@pytest.mark.parametrize('anchortype', list(AnchorType))
def test_decorator_anchortype(testchannel, anchortype):
    """Decorating a callable with an anchortype adds it as the correct task"""
    keyobj = Namespace()

    @testchannel(keyobj=keyobj, anchortype=anchortype)
    async def one():
        """one"""

    assert keyobj.key in testchannel
    assert testchannel.anchortype(keyobj.key) == anchortype
    assert testchannel[keyobj.key] is one


def test_decorator_noargs(testchannel):
    """Decorate a callable without any args"""
    assert len(testchannel) == 0

    @testchannel
    async def one():
        """one"""

    assert len(testchannel) == 1
    assert 0 in testchannel
    assert testchannel[0] is one


def test_decorator_bad_anchortype(testchannel):
    """Raise error if given a bad anchortype"""

    val = 42
    expected = ('anchortype expected AnchorType, got {} instead'.
                format(type(val).__name__))
    with pytest.raises(TypeError) as err:

        @testchannel(anchortype=val)
        async def one():
            """one"""

    assert err.value.args == (expected, )


@pytest.mark.parametrize('val', [42, 4.2, '42', [42], (42, )])
def test_decorator_noncallable(testchannel, val):
    """Raise error if given a non-callable"""
    expected = ('corofunc expected callable, got {} instead'.
                format(type(val).__name__))
    with pytest.raises(TypeError) as err:
        testchannel(val)

    assert err.value.args == (expected, )


# ============================================================================
# Test __iter__
# ============================================================================


def test_iter(testchannel):
    """Return all keys"""

    @testchannel(anchortype=AnchorType.none)
    async def one():
        """one"""

    @testchannel(anchortype=AnchorType.first)
    async def two():
        """two"""

    @testchannel(anchortype=AnchorType.last)
    async def three():
        """three"""

    assert len(testchannel) == 3
    assert set(testchannel) == set(range(3))


# ============================================================================
# Test key methods
# ============================================================================


@pytest.mark.parametrize('methodname', ['__getitem__', 'remove', '__delitem__',
                                        'anchortype'])
def test_nokey(testchannel, methodname):
    """Raise error if using non-existent key with any key-using methods"""
    key = 42

    with pytest.raises(KeyError) as err:
        getattr(testchannel, methodname)(key)

    assert err.value.args == (key, )


@pytest.mark.asyncio
async def test_nokey_aremove(testloop, testchannel):
    """Async remove key"""
    key = 42

    with pytest.raises(KeyError) as err:
        await testchannel.aremove(key)

    assert err.value.args == (key, )


# ============================================================================
# Test remove
# ============================================================================


@pytest.mark.parametrize('val', list(range(10)))
def test_remove(testchannel, val):
    """Remove key"""
    numrange = list(range(10))
    atypelist = list(AnchorType)

    for i in numrange:
        atype = choice(atypelist)
        testchannel(list, anchortype=atype)

    assert len(testchannel) == 10
    assert val in testchannel

    testchannel.remove(val)

    assert len(testchannel) == 9
    assert val not in testchannel


@pytest.mark.parametrize('val', list(range(10)))
@pytest.mark.asyncio
async def test_aremove(testchannel, val):
    """Remove key"""
    numrange = list(range(10))
    atypelist = list(AnchorType)

    async for i in aiter(numrange):
        i = i  # To stop lint tool from complaining
        atype = choice(atypelist)
        testchannel(list, anchortype=atype)

    assert len(testchannel) == 10
    assert val in testchannel

    await testchannel.aremove(val)

    assert len(testchannel) == 9
    assert val not in testchannel


# ============================================================================
# Test clear
# ============================================================================


@pytest.mark.parametrize('val', list(range(10)))
def test_clear(testchannel, val):
    """Clear all keys"""
    numrange = list(range(10))
    atypelist = list(AnchorType)

    for i in numrange:
        atype = choice(atypelist)
        testchannel(list, anchortype=atype)

    assert len(testchannel) == 10
    assert val in testchannel

    testchannel.clear()

    assert len(testchannel) == 0
    assert val not in testchannel


# ============================================================================
# Test open
# ============================================================================


def test_open_state(testchannel):
    """Opening the channel sets correct state"""

    with testchannel.open() as t:
        assert t.state == ChannelState.open

    assert testchannel.state == ChannelState.closed


@pytest.mark.parametrize('state', [
    c for c in ChannelState if c not in
    [ChannelState.closed, ChannelState.closing]
])
def test_open_alreadyopen(testchannel, state):
    """Opening an already open channel raises an error"""

    testchannel._state = state
    with pytest.raises(ChannelOpenError):
        testchannel.open()


@pytest.mark.parametrize('callit', [True, False])
def test_open_via_channel(testchannel, callit):
    """Can open via channel descriptor"""

    channel = testchannel.channel() if callit else testchannel.channel

    with channel as t:
        assert t.state == ChannelState.open

    assert testchannel.state == ChannelState.closed


# ============================================================================
# Test start
# ============================================================================


def test_start_closed(testchannel):
    """Raise error if trying to start a closed channel"""
    with pytest.raises(ChannelClosedError):
        testchannel.start()


def test_start_already_listening(testloop, testchannel):
    """Raise error if trying to start an already listening channel"""
    with testchannel.open():
        testchannel.start()
        with pytest.raises(ChannelListeningError):
            testchannel.start()


def test_start_unpause(testloop, testchannel):
    """Unpause"""

    val = []

    @testchannel
    async def one(data):
        """one"""
        vallen = len(val)
        if vallen == 5:
            testchannel.pause()
            val.append(42)
            return
        elif vallen == 6:
            testchannel.start()
        val.append(data)
        if data == 9:
            testchannel.stop()

    async def run():
        """run"""
        for i in range(10):
            asyncio.ensure_future(testchannel.send(i))
            await asyncio.sleep(0)
        await testchannel.join()

    with testchannel.open():
        testchannel.start()
        testloop.run_until_complete(run())

    expected = list(range(10))
    expected[5] = 42
    assert set(val) == set(expected)


# ============================================================================
# Test pause
# ============================================================================


def test_pause_already_closed(testchannel):
    """Raise error if pausing a channel that is already closed"""
    with pytest.raises(ChannelClosedError):
        testchannel.pause()


def test_pause_not_listening(testchannel):
    """Raise error if pausing a channel that is open but not listening"""
    with testchannel.open():
        with pytest.raises(NotListeningError):
            testchannel.pause()


# ============================================================================
# Test stop
# ============================================================================


def test_datachannel_stop_already_closed(testchannel):
    """Raise error if stopping an already closed channel"""
    with testchannel.open():
        pass

    with pytest.raises(ChannelClosedError):
        testchannel.stop()


def test_datachannel_stop_already_stopped(testchannel):
    """Do nothing if stopping an already stopped channel"""
    with testchannel.open():
        testchannel.stop()
        testchannel.stop()


# ============================================================================
# Test __getitem__
# ============================================================================


def test_getitem_cleared_key(testchannel):
    """Trying to get from the empty channel raises KeyError"""
    key = testchannel.add(list)
    testchannel.remove(key)

    with pytest.raises(KeyError) as err:
        testchannel.__getitem__(key)

    assert err.value.args == (key, )


def test_getitem_nokey(testchannel):
    """Raise error if key not found in channel with a few added callables"""
    key = testchannel.add(list)
    for i in range(5):
        testchannel.add(list)
    testchannel.remove(key)

    with pytest.raises(KeyError) as err:
        testchannel.__getitem__(key)

    assert err.value.args == (key, )


# ============================================================================
# Test anchortype
# ============================================================================


def test_anchortype_cleared_key(testchannel):
    """Trying to get anchortype from the empty channel raises KeyError"""
    key = testchannel.add(list)
    testchannel.remove(key)

    with pytest.raises(KeyError) as err:
        testchannel.anchortype(key)

    assert err.value.args == (key, )


def test_anchortype_nokey(testchannel):
    """Raise error if key not found in channel"""
    key = testchannel.add(list)
    for i in range(5):
        testchannel.add(list)
    testchannel.remove(key)

    with pytest.raises(KeyError) as err:
        testchannel.anchortype(key)

    assert err.value.args == (key, )


# ============================================================================
# Test DataChannel
# ============================================================================


@pytest.mark.parametrize('asend', [True, False])
def test_datachannel_synchronous(testloop, testchannel, asend):
    """Start the datachannel"""

    val = []
    expected = [(i, 42) for i in [1, 2, 3, 4, 6, 5]]
    stop = False

    @testchannel(anchortype=AnchorType.first)
    async def one(data):
        """one"""
        val.append((1, data))

    @testchannel(anchortype=AnchorType.first)
    async def two(data):
        """two"""
        val.append((2, data))

    @testchannel
    async def three(data):
        """three"""
        val.append((3, data))

    @testchannel
    async def four(data):
        """four"""
        val.append((4, data))

    @testchannel(anchortype=AnchorType.last)
    async def five(data):
        """five"""
        val.append((5, data))

    @testchannel(anchortype=AnchorType.last)
    async def six(data):
        """six"""
        nonlocal stop
        val.append((6, data))
        stop = True

    async def run():
        """run"""
        nonlocal stop
        assert testchannel.state == ChannelState.listening
        if asend:
            await testchannel.send(42)
        else:
            testchannel.put(42)
        while True:
            if stop:
                break
            await asyncio.sleep(0)

    with testchannel.open():
        testchannel.start(asyncfunc=False)
        testloop.run_until_complete(run())

    assert val == expected


@pytest.mark.parametrize('asend', [True, False])
def test_datachannel_async(testloop, testchannel, asend):
    """Start the datachannel (async)"""

    val = []
    dataval = 42
    stop = False

    @testchannel(anchortype=AnchorType.first)
    async def one(data):
        """one"""
        val.append((1, data))

    @testchannel(anchortype=AnchorType.first)
    async def two(data):
        """two"""
        val.append((2, data))

    @testchannel
    async def three(data):
        """three"""
        val.append((3, data))

    @testchannel
    async def four(data):
        """four"""
        val.append((4, data))

    @testchannel(anchortype=AnchorType.last)
    async def five(data):
        """five"""
        val.append((5, data))

    @testchannel(anchortype=AnchorType.last)
    async def six(data):
        """six"""
        nonlocal stop
        val.append((6, data))
        stop = True

    async def run():
        """run"""
        nonlocal stop
        assert testchannel.state == ChannelState.listening
        assert testchannel.isopen()
        if asend:
            await testchannel.send(dataval)
        else:
            testchannel.put(dataval)
        while True:
            if stop:
                break
            await asyncio.sleep(0)

    assert not testchannel.isopen()
    with testchannel.open():
        testchannel.start()
        testloop.run_until_complete(run())
    assert not testchannel.isopen()

    first = set((i, dataval) for i in [1, 2])
    middle = set((i, dataval) for i in [3, 4])
    last = set((i, dataval) for i in [5, 6])

    assert set(val[0:2]) == first
    assert set(val[2:4]) == middle
    assert set(val[4:6]) == last


def test_datachannel_pause(testloop, testchannel):
    """Pause channel"""

    val = []

    @testchannel
    async def one(data):
        """one"""
        val.append((1, data))
        testchannel.pause()
        assert testchannel.state == ChannelState.paused
        val.append('pause')

    async def run():
        """run"""
        async for i in aiter(range(10)):
            await testchannel.send(i)
            await asyncio.sleep(0)

    with testchannel.open():
        testchannel.start()
        testloop.run_until_complete(run())

    assert len(val) % 2 == 0
    assert len(val) < 20
    if val:
        assert val[:2] == [(1, 0), 'pause']


def test_datachannel_stop(testloop, testchannel):
    """Stop channel"""
    val = []

    @testchannel
    async def one(data):
        """one"""
        val.append((1, data))

    async def stoppedsend(data):
        """stoppedsend"""
        await testchannel.send(data)

    async def run():
        """run"""
        for i in range(10):
            await testchannel.send(i)
        testchannel.stop()
        await testchannel.join()

    with testchannel.open():
        testchannel.start()
        testloop.run_until_complete(run())


def test_datachannel_close(testloop, testchannel):
    """Close channel while it's listening"""

    val = []

    @testchannel
    async def one(data):
        """one"""
        if testchannel.state == ChannelState.closed:
            return
        val.append((1, data))
        testchannel.close()
        assert testchannel.state == ChannelState.closed
        val.append('close')

    async def run():
        """run"""
        async for i in aiter(range(10)):
            try:
                testchannel.put(i)
            except ChannelClosedError:
                pass
            await asyncio.sleep(0)

    with testchannel.open():
        testchannel.start()
        testloop.run_until_complete(run())
        assert testchannel.state == ChannelState.closed

    assert val
    assert val == [(1, 0), 'close']


def test_datachannel_send_wait(testloop, testchannel):
    """Send blocks when channel is closed until it is opened"""

    val = []

    @testchannel
    async def one(data):
        """one"""
        if len(val) == 5:
            testchannel.stop()
            return
        val.append(data)

    async def run():
        """run"""
        async for i in aiter(range(10)):
            asyncio.ensure_future(testchannel.send(i))
            await asyncio.sleep(0)
        await testchannel.join()

    with testchannel.open():
        testchannel.start()
        testloop.run_until_complete(run())

    assert val == list(range(5))


def test_datachannel_send_wait_notopened(testloop, testchannel):
    """Send blocks when channel has never been opened"""

    val = []

    @testchannel
    async def one(data):
        """one"""
        val.append(data)

    async def run():
        """run"""
        async for i in aiter(range(10)):
            asyncio.ensure_future(testchannel.send(i))
            await asyncio.sleep(0)
        await testchannel.join()

    testloop.run_until_complete(run())

    assert not val


def test_datachannel_multiple_shutdown(caplog, testloop, testchannel):
    """Shutdown channel can be called multiple times"""
    val = 0

    @testchannel
    async def one(data):
        """one"""
        nonlocal val
        val += 1

    async def run():
        """run"""
        for i in range(10):
            await testchannel.send(i)
        await testchannel.shutdown()
        await testchannel.shutdown()

    level = LogLevel.DEBUG
    with caplog.at_level(level.value):
        with testchannel.open():
            testchannel.start()
            testloop.run_until_complete(run())

        records = [r for r in caplog.records if r.name != 'asyncio'
                   and r.message.endswith(': shutdown')]
        assert len(records) == 2


# ============================================================================
# General
# ============================================================================


def test_no_listeners(testloop, testchannel):
    """Starting the channel without any listeners"""

    async def run():
        """run"""
        async for i in aiter(range(10)):
            await testchannel.send(i)
            await asyncio.sleep(0)

    with testchannel.open():
        testchannel.start(asyncfunc=False)
        testloop.run_until_complete(run())


@pytest.mark.parametrize('name', [None, 'hello'])
def test_name(name):
    """Retrieve the channel's name"""
    expected = 'datachannel' if name is None else name
    c = DataChannel(name=name)
    assert c.name == expected


# ============================================================================
# Cleanup channel
# ============================================================================


@pytest.mark.parametrize('val', [42, 4.2, '42', [42]])
def test_cleanup_notchannel(val, testchannel):
    """Raise error if cleanup arg given non-DataChannel object"""
    expected = ('cleanup expected DataChannel object, got {} instead'.
                format(type(val).__name__))
    with pytest.raises(TypeError) as err:
        testchannel.open(cleanup=val)

    assert err.value.args == (expected, )


def test_cleanup_send_qsize(testloop, testchannel):
    """Send qsize into cleanup channel"""

    val = []
    expected = list(reversed(range(10)))
    cleanup = DataChannel()

    @cleanup
    async def getqsize(qsize):
        val.append(qsize)

    async def run():
        for i in range(1000):
            await testchannel.send(i)
        await testchannel.shutdown()

    with ExitStack() as stack:
        stack.enter_context(cleanup.open())
        stack.enter_context(testchannel.open(maxsize=10, cleanup=cleanup))

        cleanup.start()
        testchannel.start()
        testloop.run_until_complete(run())

    assert val == expected


# ============================================================================
#
# ============================================================================
