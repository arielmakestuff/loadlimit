# -*- coding: utf-8 -*-
# test/unit/channel/test_commandchannel.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test CommmandChannel"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from asyncio import Queue, PriorityQueue, LifoQueue
from enum import Enum

# Third-party imports
import pytest

# Local imports
from loadlimit.channel import AnchorType, CommandChannel


# ============================================================================
# Globals
# ============================================================================


class TempCommand(Enum):
    """TempCommand"""
    none = 1
    hello = 2
    world = 3
    answer = 42


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def testchannel():
    """Return CommandChannel object"""
    return CommandChannel(TempCommand)


# ============================================================================
# Test init
# ============================================================================


@pytest.mark.parametrize('qcls', [Queue, PriorityQueue, LifoQueue])
def test_init_maxsize_one(qcls):
    """Always creates internal queue with maxsize 1"""
    c = CommandChannel(TempCommand, queuecls=qcls)
    with c.open():
        assert c._queue.maxsize == 1


@pytest.mark.parametrize('val', [int, 42, TempCommand.none])
def test_init_bad_cmdenum(val):
    """Raise error if cmdenum is not an Enum subclass"""
    expected = ('cmdenum expected Enum subclass, got {}'.
                format(type(val).__name__))
    with pytest.raises(TypeError) as err:
        CommandChannel(val)

    assert err.value.args == (expected, )


def test_init_no_none_member():
    """Raise error if the cmdenum Enum does not have a member named 'none'"""
    test = Enum('test', ['one'])

    expected = "'none' member of {} enum not found".format(test.__name__)
    with pytest.raises(ValueError) as err:
        CommandChannel(test)

    assert err.value.args == (expected, )


# ============================================================================
# Test __getitem__
# ============================================================================


def test_getitem_notasks(testchannel):
    """Raise error when trying to getitem w/ no corofunc added"""
    key = 42
    with pytest.raises(KeyError) as err:
        testchannel.__getitem__(key)

    assert err.value.args == (key, )


def test_getitem_tasks_nokey(testchannel):
    """Raise error if key doesn't exist w/ corofuncs added"""
    testchannel(int)
    testchannel(list, command=TempCommand.hello)
    testchannel(dict, command=TempCommand.world)
    testchannel(tuple, command=TempCommand.answer)

    key = 42

    assert len(testchannel) == 4
    with pytest.raises(KeyError) as err:
        testchannel.__getitem__(key)

    assert err.value.args == (key, )


@pytest.mark.parametrize('key', list(range(4)))
def test_getitem_tasks_key(testchannel, key):
    """Return stored func"""
    funclist = [int, list, dict, tuple]
    for i, func in zip(TempCommand, funclist):
        testchannel(func, command=i)

    assert len(testchannel) == 4
    assert testchannel.__getitem__(key) is funclist[key]


# ============================================================================
# Test __iter__
# ============================================================================


def test_iter(testchannel):
    """Iterate over all keys"""
    testchannel(int)
    testchannel(list, command=TempCommand.hello)
    testchannel(dict, command=TempCommand.world)
    testchannel(tuple, command=TempCommand.answer)

    expected = set(range(4))
    assert expected == set(testchannel)


# ============================================================================
# Test __contains__
# ============================================================================


@pytest.mark.parametrize('key', list(range(4)))
def test_contains(testchannel, key):
    """Return True when key is in the channel"""
    testchannel(int)
    testchannel(list, command=TempCommand.hello)
    testchannel(dict, command=TempCommand.world)
    testchannel(tuple, command=TempCommand.answer)

    assert testchannel.__contains__(key)


def test_contains_nokey(testchannel):
    """Return False if key is not in the channel"""
    testchannel(int)
    testchannel(list, command=TempCommand.hello)
    testchannel(dict, command=TempCommand.world)
    testchannel(tuple, command=TempCommand.answer)

    key = 42

    assert not testchannel.__contains__(key)


# ============================================================================
# Test __len__
# ============================================================================


def test_len(testchannel):
    """Return number of added funcs"""
    testchannel(int)
    testchannel(list, command=TempCommand.hello)
    testchannel(dict, command=TempCommand.world)
    testchannel(tuple, command=TempCommand.answer)

    assert len(testchannel) == 4


def test_len_nofunc(testchannel):
    """Return 0 if nothing has been added"""
    assert len(testchannel) == 0


# ============================================================================
# Test anchortype
# ============================================================================


@pytest.mark.parametrize('populate', [True, False])
def test_anchortype_nokey(testchannel, populate):
    """Raise error if key not in channel"""
    if populate:
        testchannel(int)
        testchannel(list, command=TempCommand.hello,
                    anchortype=AnchorType.first)
        testchannel(dict, command=TempCommand.world,
                    anchortype=AnchorType.last)
        testchannel(tuple, command=TempCommand.answer)

    key = 42
    with pytest.raises(KeyError) as err:
        testchannel.anchortype(key)

    assert err.value.args == (key, )


@pytest.mark.parametrize('key', list(range(4)))
def test_anchortype_key(testchannel, key):
    """Return anchortype of the given key"""
    funclist = [int, list, dict, tuple]
    a = [AnchorType.none, AnchorType.first, AnchorType.last, AnchorType.none]
    c = [TempCommand.none, TempCommand.hello, TempCommand.world,
         TempCommand.answer]

    for func, command, atype in zip(funclist, c, a):
        testchannel(func, command=command, anchortype=atype)

    assert testchannel.anchortype(key) == a[key]


# ============================================================================
# Test findcommand
# ============================================================================


@pytest.mark.parametrize('populate', [True, False])
def test_findcommand_tasks_nokey(testchannel, populate):
    """Raise error if key doesn't exist w/ corofuncs added"""
    if populate:
        testchannel(int)
        testchannel(list, command=TempCommand.hello)
        testchannel(dict, command=TempCommand.world)
        testchannel(tuple, command=TempCommand.answer)

    key = 42
    with pytest.raises(KeyError) as err:
        testchannel.findcommand(key)

    assert err.value.args == (key, )


@pytest.mark.parametrize('key', list(range(4)))
def test_findcommand_tasks_key(testchannel, key):
    """Return stored func"""
    funclist = [int, list, dict, tuple]
    commands = list(TempCommand)
    for i, func in zip(commands, funclist):
        testchannel(func, command=i)

    assert len(testchannel) == 4
    assert testchannel.findcommand(key) == commands[key]


# ============================================================================
# Test add
# ============================================================================


@pytest.mark.parametrize('val', [42, 4.2, '42', [42], {}, ()])
def test_add_badcommand(testchannel, val):
    """Raise error if bad command value given"""
    expected = ('command expected {}, got {} instead'.
                format(TempCommand.__name__, type(val).__name__))
    with pytest.raises(TypeError) as err:
        testchannel.add(list, command=val)

    assert err.value.args == (expected, )


def test_add_defaultcommand(testchannel):
    """Default command value is cmdenum.none"""
    testchannel(int)
    assert testchannel.findcommand(0) == TempCommand.none


# ============================================================================
# Test remove
# ============================================================================


@pytest.mark.parametrize('populate', [True, False])
def test_remove_tasks_nokey(testchannel, populate):
    """Raise error if key doesn't exist w/ corofuncs added"""
    if populate:
        testchannel(int)
        testchannel(list, command=TempCommand.hello)
        testchannel(dict, command=TempCommand.world)
        testchannel(tuple, command=TempCommand.answer)

    key = 42
    with pytest.raises(KeyError) as err:
        testchannel.remove(key)

    assert err.value.args == (key, )


@pytest.mark.parametrize('key', list(range(4)))
def test_remove_tasks_key(testchannel, key):
    """Return stored func"""
    funclist = [int, list, dict, tuple]
    commands = list(TempCommand)
    for i, func in zip(commands, funclist):
        testchannel(func, command=i)

    assert len(testchannel) == 4
    testchannel.remove(key)
    assert key not in testchannel


# ============================================================================
# Test aremove
# ============================================================================


@pytest.mark.parametrize('populate', [True, False])
@pytest.mark.asyncio
async def test_aremove_tasks_nokey(testchannel, populate):
    """Raise error if key doesn't exist w/ corofuncs added"""
    if populate:
        testchannel(int)
        testchannel(list, command=TempCommand.hello)
        testchannel(dict, command=TempCommand.world)
        testchannel(tuple, command=TempCommand.answer)

    key = 42
    with pytest.raises(KeyError) as err:
        await testchannel.aremove(key)

    assert err.value.args == (key, )


@pytest.mark.parametrize('key', list(range(4)))
@pytest.mark.asyncio
async def test_aremove_tasks_key(testchannel, key):
    """Return stored func"""
    funclist = [int, list, dict, tuple]
    commands = list(TempCommand)
    for i, func in zip(commands, funclist):
        testchannel(func, command=i)

    assert len(testchannel) == 4
    await testchannel.aremove(key)
    assert key not in testchannel


# ============================================================================
# Test send
# ============================================================================


@pytest.mark.parametrize('val', [42, 4.2, '42', [42], {}, ()])
@pytest.mark.asyncio
async def test_send_badcommand(testchannel, val):
    """Raise error if try to send a non-command"""
    expected = ('command expected {}, got {} instead'.
                format(TempCommand.__name__, type(val).__name__))
    with testchannel.open():
        testchannel.start()
        with pytest.raises(TypeError) as err:
            await testchannel.send(val)

    assert err.value.args == (expected, )


@pytest.mark.parametrize('val', [42, 4.2, '42', [42], {}, ()])
def test_put_badcommand(testloop, testchannel, val):
    """Raise error if try to send a non-command"""
    expected = ('command expected {}, got {} instead'.
                format(TempCommand.__name__, type(val).__name__))
    with testchannel.open():
        testchannel.start()
        with pytest.raises(TypeError) as err:
            testchannel.put(val)

    assert err.value.args == (expected, )


@pytest.mark.parametrize('asyncfunc', [True, False])
def test_put_command(testloop, testchannel, asyncfunc):
    """Put a command into the channel"""

    val = []

    @testchannel(command=TempCommand.answer)
    async def one(command):
        """one"""
        val.append(command)

    async def run():
        """run"""
        testchannel.put(TempCommand.answer)
        await asyncio.sleep(0.1)

    with testchannel.open():
        testchannel.start(asyncfunc=asyncfunc)
        testloop.run_until_complete(run())

    assert val == [TempCommand.answer]


# ============================================================================
# Test run channel, no tasks
# ============================================================================


@pytest.mark.parametrize('asyncfunc', [True, False])
def test_run_notasks(testloop, testchannel, asyncfunc):
    """Run the loop without any tasks added"""

    async def run():
        """run"""
        await testchannel.send(TempCommand.answer)
        await asyncio.sleep(0.1)

    with testchannel.open():
        testchannel.start(asyncfunc=asyncfunc)
        testloop.run_until_complete(run())


@pytest.mark.parametrize('asyncfunc', [True, False])
def test_run_tasks(testloop, testchannel, asyncfunc):
    """Run the loop with tasks added"""
    val = []

    @testchannel(command=TempCommand.answer)
    async def one(command):
        """one"""
        val.append(command)

    async def run():
        """run"""
        await testchannel.send(TempCommand.answer)
        await asyncio.sleep(0.1)

    with testchannel.open():
        testchannel.start(asyncfunc=asyncfunc)
        testloop.run_until_complete(run())

    assert val == [TempCommand.answer]


@pytest.mark.parametrize('asyncfunc', [True, False])
def test_run_notcommand(testloop, testchannel, asyncfunc):
    """Run the loop with tasks added"""
    val = []

    @testchannel(command=TempCommand.answer)
    async def one(command):
        """one"""
        val.append(command)

    @testchannel(command=TempCommand.world)
    async def two(command):
        """two"""
        val.append(command)

    async def run():
        """run"""
        await testchannel.send(TempCommand.world)
        await asyncio.sleep(0.1)

    with testchannel.open():
        testchannel.start(asyncfunc=asyncfunc)
        testloop.run_until_complete(run())

    assert val == [TempCommand.world]


# @pytest.mark.parametrize('asyncfunc', [True, False])
# def test_run_tasks(testloop, testchannel, asyncfunc):
#     """Run the loop with tasks added"""
#     val = []

#     @testchannel(command=TempCommand.answer)
#     async def one(command):
#         """one"""
#         val.append(command)

#     async def run():
#         """run"""
#         await testchannel.send(TempCommand.answer)
#         await asyncio.sleep(0.1)

#     with testchannel.open():
#         testchannel.start(asyncfunc=asyncfunc)
#         testloop.run_until_complete(run())

#     assert val == [TempCommand.answer]


# ============================================================================
#
# ============================================================================
