# -*- coding: utf-8 -*-
# test/unit/stat/test_countstore.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test Count and CountStore classes"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from asyncio import iscoroutinefunction
from collections import defaultdict,OrderedDict
from functools import partial
from time import perf_counter

# Third-party imports
import pytest

# Local imports
import loadlimit.stat as stat
from loadlimit.stat import Count, CountStore
from loadlimit.util import now


# ============================================================================
# Test Count
# ============================================================================


def test_count_init():
    """Initialize Count objects with attrs set to 0"""
    c = Count()
    assert c.success == 0
    assert c.window_success == 0
    for attr in ['error', 'failure']:
        assert isinstance(getattr(c, attr), defaultdict)


def test_count_newerror():
    """Adding a new error initializes its count to 0"""
    c = Count()
    assert c.error[42] == 0
    assert c.window_error[4242] == 0


def test_count_newfailure():
    """Adding a new failure initializes its count to 0"""
    c = Count()
    assert c.failure[42] == 0
    assert c.window_failure[4242] == 0


@pytest.mark.parametrize('val', [1, 2, 42])
def test_count_addsuccess(val):
    """Adding a success increments the success count"""
    c = Count()
    func = c.addsuccess if val == 1 else partial(c.addsuccess, val)
    for i in range(10):
        c.success = 0
        func()
        assert c.success == val


@pytest.mark.parametrize('val', [1, 2, 42])
def test_count_adderror(val):
    """Adding an error increments the error count"""
    c = Count()
    func = c.adderror if val == 1 else partial(c.adderror, val=val)
    key = 42
    for i in range(10):
        c.error.clear()
        func(key)
        assert c.error[key] == val


@pytest.mark.parametrize('val', [1, 2, 42])
def test_count_addfailure(val):
    """Adding a failure increments the failure count"""
    c = Count()
    func = c.addfailure if val == 1 else partial(c.addfailure, val=val)
    key = 42
    for i in range(10):
        c.failure.clear()
        func(key)
        assert c.failure[key] == val


def test_count_addclient():
    """Adding a client adds the client id"""
    c = Count()
    for i in range(10):
        clientid = id(i)
        c.addclient(clientid)
        assert clientid in c.client
        assert len(c.client) == i + 1


def test_count_resetclient_noframes():
    """Remove all clients on reset"""
    c = Count()
    c.window_success = 42
    c.window_error[42] = 42
    c.window_failure[42] = 42
    c.window_end = 42
    c.window_start = 42

    for i in range(10):
        clientid = id(i)
        c.addclient(clientid)

    assert len(c.client) == 10
    cursetid = id(c.client)

    c.resetclient()

    assert id(c.client) != cursetid
    assert not c.client
    assert isinstance(c.client, set)

    assert c.window_start is None
    assert c.window_end is None
    assert isinstance(c.window_failure, defaultdict)
    assert isinstance(c.window_error, defaultdict)
    assert not c.window_failure
    assert not c.window_error


def test_count_resetclient_frames():
    """Remove all clients on reset"""
    c = Count()
    c.window_success = 42
    c.window_error[42] = 42
    c.window_failure[42] = 42
    c.window_end = 42
    c.window_start = 42
    c.frames = {42: Count()}
    c.frames[42].window_start = 4242

    for i in range(10):
        clientid = id(i)
        c.addclient(clientid)

    assert len(c.client) == 10
    cursetid = id(c.client)

    c.resetclient()

    assert id(c.client) != cursetid
    assert not c.client
    assert isinstance(c.client, set)

    assert c.window_start == 4242
    assert c.window_end is None
    assert isinstance(c.window_failure, defaultdict)
    assert isinstance(c.window_error, defaultdict)
    assert not c.window_failure
    assert not c.window_error


def test_count_sum_zero():
    """Return 0"""
    c = Count()
    assert c.sum() == 0


@pytest.mark.parametrize('success,error,failure', [
    (s, e, f) for s in [True, False]
    for e in [True, False]
    for f in [True, False]
])
def test_count_sum(success, error, failure):
    """Returns the correct sum"""
    c = Count()
    expected = 0
    if success:
        c.addsuccess()
        expected += 1
    if error:
        c.adderror('error')
        expected += 1
    if c.failure:
        c.addfailure('failure')
        expected += 1

    assert c.sum() == expected


def test_count_update_client():
    """Update client with values from old client"""
    old = Count()
    new = Count()

    old.client.update(range(5))
    new.client.add(42)

    new.update(old)
    assert new.client == set([42] + list(range(5)))


def test_count_update_success():
    """Add success val from old"""
    old = Count()
    new = Count()

    old.success = 40
    new.success = 2

    new.update(old)
    assert new.success == 42


def test_count_update_errors():
    """Add error vals from old"""
    old = Count()
    new = Count()

    old.error[42] = 40
    new.error[42] = 2

    new.update(old)
    assert list(new.error.items()) == [(42, 42)]


def test_count_update_failures():
    """Add failure vals from old"""
    old = Count()
    new = Count()

    old.failure[42] = 40
    new.failure[42] = 2

    new.update(old)
    assert list(new.failure.items()) == [(42, 42)]


# ============================================================================
# Test CountStore.__init__
# ============================================================================


def test_countstore_init_default():
    """New keys are automatically initialized with a Count object"""
    c = CountStore()
    assert not c
    assert isinstance(c[42], Count)
    assert c
    assert len(c) == 1


# ============================================================================
# Test CountStore.__call__
# ============================================================================


def test_countstore_call_noname():
    """Raise error if not called with a name"""

    c = CountStore()
    expected = 'name not given'
    with pytest.raises(ValueError) as err:
        c()

    assert err.value.args == (expected, )


@pytest.mark.parametrize('val', [42, 4.2, [42], {}, ()])
def test_countstore_call_name_notstr(val):
    """Raise error if given a non-str name"""
    expected = 'name expected str, got {} instead'.format(type(val).__name__)
    c = CountStore()
    with pytest.raises(TypeError) as err:
        c(name=val)

    assert err.value.args == (expected, )


def test_countstore_call_decorator():
    """Calling immediately with corofunc"""

    measure = CountStore()

    async def one():
        """one"""

    wrapped = measure(one, name='one')

    assert wrapped is not one
    assert iscoroutinefunction(wrapped)
    assert hasattr(wrapped, '__wrapped__')
    assert wrapped.__wrapped__ is one


# ============================================================================
# Test CountStore.__call__ measurement
# ============================================================================


@pytest.fixture
def measure():
    """docstring for mkcountstore"""
    return CountStore()


@pytest.fixture
def noopfunc(measure):
    """docstring for mknoop"""
    called = []

    @measure(name='run')
    async def noop(err=None):
        """Do nothing"""
        called.append(True)
        if err is not None:
            raise err

    return noop, called


@pytest.mark.asyncio
async def test_countstore_measure_setkey(measure, noopfunc):
    """Adds name as a CountStore key"""
    noop, called = noopfunc
    assert not measure

    await noop()

    # The 'run' key was added
    assert len(measure) == 1
    assert list(measure.keys()) == ['run']
    assert len(called) == 1 and called[0] is True

    run = measure['run']
    assert isinstance(run, Count)


async def test_countstore_measure_window_frames(measure, noopfunc):
    """Window frames container is an OrderedDict"""
    noop, called = noopfunc
    assert not measure

    await noop()

    run = measure['run']
    assert run.frames is not None
    assert isinstance(run.frames, OrderedDict)

    # No frames should be stored since the frame was completed
    assert not run.frames

    # assert not run.frames
    # assert isinstance(run.window_start, float)
    # assert run.window_start > 0

    # assert measure.start == run.window_start

    # assert run.sum() == 1
    # assert run.success == 1
    # assert len(run.client) == 1
    # assert run.window_success == 1
    # assert not run.window_error
    # assert not run.window_failure
    # assert not run.error
    # assert not run.failure
    # assert run.window_end > run.window_start

    # assert run.window_start is not None
    # assert isinstance(run.window_start, float)
    # assert run.window_start > 0


async def test_countstore_measure_window_frames_alreadyexist(measure, noopfunc):
    """Window frames container is not overwritten"""
    noop, called = noopfunc
    measure['run'].frames = dict(hello='world')
    assert not measure

    await noop()

    run = measure['run']
    assert run.frames is not None
    assert isinstance(run.frames, dict)
    assert set(run.frames) == {'run', 'hello'}


async def test_countstore_measure_window_start_none(measure, noopfunc):
    """window_start is set to same value as start"""
    noop, called = noopfunc
    assert not measure

    await noop()

    assert measure.window_start == measure.start


async def test_countstore_measure_window_start_notnone(measure, noopfunc):
    """window_start is not changed"""
    noop, called = noopfunc
    measure.window_start = 'HELLO'
    assert not measure

    await noop()

    assert measure.window_start == 'HELLO'


async def test_countstore_measure_start_none(measure, noopfunc):
    """start is set to same value as window_start"""
    noop, called = noopfunc
    assert not measure

    await noop()

    assert measure.start == measure.window_start


async def test_countstore_measure_start_notnone(measure, noopfunc):
    """start is not changed"""
    noop, called = noopfunc
    measure.start = 'HELLO'
    assert not measure

    await noop()

    assert measure.start == 'HELLO'


@pytest.mark.asyncio
async def test_countstore_measure_failure(measure, noopfunc):
    """Failure is measured"""
    noop, called = noopfunc
    fail = stat.Failure(42)

    assert not measure

    await noop(err=fail)

    assert called == [True]
    count = measure['run']
    assert count.success == 0
    assert count.window_success == 0
    assert not count.error
    assert not count.window_error

    assert len(count.failure) == 1
    assert len(count.window_failure) == 1
    key = str(fail.args[0])
    assert count.failure[key] == 1
    assert count.window_failure[key] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
async def test_countstore_measure_error(measure, noopfunc, exctype):
    """Errors are measured"""
    noop, called = noopfunc
    err = exctype(42)

    assert not measure

    await noop(err=err)

    assert called == [True]
    count = measure['run']
    assert count.success == 0
    assert count.window_success == 0
    assert not count.failure
    assert not count.window_failure

    assert len(count.error) == 1
    assert len(count.window_error) == 1
    key = repr(err)
    assert count.error[key] == 1
    assert count.window_error[key] == 1


@pytest.mark.skipif(True, reason='dev')
def test_countstore_window_client_getter_zero():
    """Return 0 if _window_client attr is empty"""
    store = CountStore()
    c = store['run']
    assert c.window_client == 0


@pytest.mark.skipif(True, reason='dev')
def test_countstore_window_client_getter_lastitem():
    """Return value of last item in _window_client list"""
    store = CountStore()
    c = store['run']
    c._window_client.extend([0, 1, 3, 42])
    assert c.window_client == 42


@pytest.mark.skipif(True, reason='dev')
def test_countstore_window_client_setter_add():
    """Add value to _window_client if _addto_window_client is True"""
    store = CountStore()
    c = store['run']
    c._addto_window_client = True
    c.window_client = 42
    assert c._window_client == [42]
    assert c._addto_window_client is False


@pytest.mark.skipif(True, reason='dev')
def test_countstore_window_client_setter_overwrite():
    """Overwrite last value of _window_client if _addto_window_client is False"""
    store = CountStore()
    c = store['run']
    c._addto_window_client = False
    c._window_client.extend(range(5))
    c.window_client = 42
    assert c._addto_window_client is False
    assert c._window_client == list(range(4)) + [42]


@pytest.mark.skipif(True, reason='dev')
def test_countstore_pop_window_client_novals():
    """Return 0 if _window_client is empty"""
    store = CountStore()
    c = store['run']
    ret = c.pop_window_client()
    assert ret == 0


@pytest.mark.skipif(True, reason='dev')
def test_countstore_pop_window_client_1val():
    """Return value of single item in _window_client"""
    store = CountStore()
    c = store['run']
    c.window_client = 42
    assert c._addto_window_client is False

    ret = c.pop_window_client()
    assert ret == 42
    assert not c._window_client
    assert c._addto_window_client is True


@pytest.mark.skipif(True, reason='dev')
def test_countstore_pop_window_client_multivals():
    """Return sum of all items in _window_client with multiple items"""
    store = CountStore()
    c = store['run']
    c._window_client.extend(range(5))
    c._window_client.append(42)

    ret = c.pop_window_client()
    assert ret == sum(range(5)) + 42
    assert not c._window_client
    assert c._addto_window_client is True


# ============================================================================
# Test CountStore.allresetclient()
# ============================================================================


def test_count_allresetclient_reset_empty():
    """Does not raise an error when calling on an empty CountStore object"""
    c = CountStore()
    c.allresetclient()


def test_count_allresetclient_call():
    """Call resetclient() on every stored Count object"""
    c = CountStore()
    for i in range(10):
        c[i].client.update(id(i) for i in range(5))

    c.allresetclient()

    assert len(c) == 10
    assert list(c.keys()) == list(range(10))
    assert all(isinstance(count.client, set) for count in c.values())
    assert all(len(count.client) == 0 for count in c.values())


# ============================================================================
# Test CountStore.reset()
# ============================================================================


def test_countstore_reset_empty():
    """Does not raise an error on an empty CountStore"""
    c = CountStore()
    c.reset()


def test_countstore_reset_dates():
    """Dates attrs are all set to None"""
    c = CountStore()
    c.start = 42
    c.start_date = now()
    c.end = 52
    c.end_date = now()

    c.reset()

    assert c.start is None
    c.start_date is None
    c.end is None
    c.end_date is None


def test_countstore_reset_clear():
    """The dict is cleared"""
    c = CountStore()
    c['hello'].success = 9001
    c['hello'].error['first'] = 500
    c['hello'].failure['second'] = 500

    c['world'].success = 19001
    c['world'].error['first'] = 1500
    c['world'].failure['second'] = 1500

    assert c
    assert len(c) == 2

    c.reset()

    assert not c
    assert len(c) == 0


# ============================================================================
#
# ============================================================================
