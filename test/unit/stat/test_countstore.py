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
from collections import defaultdict

# Third-party imports
import pytest

# Local imports
from loadlimit.channel import DataChannel
import loadlimit.stat as stat
from loadlimit.stat import Count, CountStore
from loadlimit.util import now


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def fake_recordperiod(monkeypatch):
    fake_recordperiod = DataChannel(name='recordperiod')
    monkeypatch.setattr(stat, 'recordperiod', fake_recordperiod)


# ============================================================================
# Test Count
# ============================================================================


def test_count_init():
    """Initialize Count objects with attrs set to 0"""
    c = Count()
    assert c.success == 0
    for attr in ['error', 'failure']:
        assert isinstance(getattr(c, attr), defaultdict)


def test_count_newerror():
    """Adding a new error initializes its count to 0"""
    c = Count()
    assert c.error[42] == 0


def test_count_newfailure():
    """Adding a new failure initializes its count to 0"""
    c = Count()
    assert c.failure[42] == 0


def test_count_addsuccess():
    """Adding a success increments the success count"""
    c = Count()
    for i in range(10):
        c.addsuccess()
        assert c.success == i + 1


def test_count_adderror():
    """Adding an error increments the error count"""
    c = Count()
    key = 42
    for i in range(10):
        c.adderror(key)
        assert c.error[key] == i + 1


def test_count_addfailure():
    """Adding a failure increments the failure count"""
    c = Count()
    key = 42
    for i in range(10):
        c.addfailure(key)
        assert c.failure[key] == i + 1


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


@pytest.mark.asyncio
@pytest.mark.usefixtures('fake_recordperiod')
async def test_countstore_measure_setkey(monkeypatch):
    """Adds name as a CountStore key"""
    measure = CountStore()
    called = False

    @measure(name='run')
    async def noop():
        """Do nothing"""
        nonlocal called
        called = True

    assert not measure

    with stat.recordperiod.open() as r:
        r.start()
        await noop()

    assert len(measure) == 1
    assert list(measure.keys()) == ['run']
    assert called is True
    assert measure['run'].sum() == 1
    assert measure['run'].success == 1


@pytest.mark.asyncio
@pytest.mark.usefixtures('fake_recordperiod')
async def test_countstore_measure_initstart(monkeypatch):
    """CountStore.start is set with a float"""

    measure = CountStore()
    called = False

    @measure(name='run')
    async def noop():
        """Do nothing"""
        nonlocal called
        called = True

    assert not measure

    with stat.recordperiod.open() as r:
        r.start()
        await noop()

    assert measure.start is not None
    assert isinstance(measure.start, float)
    assert measure.start > 0

    start_date = measure.start_date
    cur = now()
    assert start_date is not None
    assert cur >= start_date
    assert start_date.floor('D') == cur.floor('D')


@pytest.mark.asyncio
@pytest.mark.usefixtures('fake_recordperiod')
async def test_countstore_measure_noinitstart(monkeypatch):
    """CountStore.start is not set if it contains a non-None value"""
    measure = CountStore()
    measure.start = 42
    called = False

    @measure(name='run')
    async def noop():
        """Do nothing"""
        nonlocal called
        called = True

    assert not measure

    with stat.recordperiod.open() as r:
        r.start()
        await noop()

    assert called is True
    assert measure.start == 42
    assert measure.start_date is None


@pytest.mark.asyncio
@pytest.mark.usefixtures('fake_recordperiod')
async def test_countstore_measure_failure():
    """Failure is measured"""
    measure = CountStore()
    called = False
    fail = stat.Failure(42)

    @measure(name='run')
    async def noop():
        """Do nothing"""
        nonlocal called
        called = True
        raise fail

    assert not measure

    with stat.recordperiod.open() as r:
        r.start()
        await noop()

    assert called is True
    count = measure['run']
    assert count.success == 0
    assert not count.error
    assert len(count.failure) == 1
    assert count.failure[str(fail.args[0])] == 1


@pytest.mark.asyncio
@pytest.mark.usefixtures('fake_recordperiod')
@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
async def test_countstore_measure_error(exctype):
    """Errors are measured"""
    measure = CountStore()
    called = False
    err = exctype(42)

    @measure(name='run')
    async def noop():
        """Do nothing"""
        nonlocal called
        called = True
        raise err

    assert not measure

    with stat.recordperiod.open() as r:
        r.start()
        await noop()

    assert called is True
    count = measure['run']
    assert count.success == 0
    assert not count.failure
    assert len(count.error) == 1
    assert count.error[repr(err)] == 1


@pytest.mark.asyncio
@pytest.mark.usefixtures('fake_recordperiod')
async def test_countstore_measure_record_data():
    """Measured data is sent via recordperiod channel"""
    measure = CountStore()
    noopcalled = False
    checkdata = None

    @stat.recordperiod
    async def check(data):
        nonlocal checkdata
        checkdata = data

    @measure(name='run')
    async def noop():
        """Do nothing"""
        nonlocal noopcalled
        noopcalled = True

    assert not measure

    with stat.recordperiod.open() as r:
        r.start()
        await noop()
        await stat.recordperiod.join()

    assert noopcalled is True
    assert checkdata is not None

    end_date = now()
    assert isinstance(checkdata, stat.CountStoreData)

    # Check end
    assert end_date >= checkdata.end
    assert checkdata.end.floor('D') == end_date.floor('D')

    # Check rate
    assert isinstance(checkdata.rate, float)
    assert checkdata.rate > 0

    # Check error and failure
    assert checkdata.error is None
    assert checkdata.failure is None


@pytest.mark.asyncio
@pytest.mark.usefixtures('fake_recordperiod')
@pytest.mark.parametrize('exctype', [Exception, RuntimeError, ValueError])
async def test_countstore_measure_record_error(exctype):
    """Include error in data sent via recordperiod channel"""
    measure = CountStore()
    noopcalled = False
    checkdata = None
    err = exctype(42)

    @stat.recordperiod
    async def check(data):
        nonlocal checkdata
        checkdata = data

    @measure(name='run')
    async def noop():
        """Do nothing"""
        nonlocal noopcalled
        noopcalled = True
        raise err

    assert not measure

    with stat.recordperiod.open() as r:
        r.start()
        await noop()
        await stat.recordperiod.join()

    assert noopcalled is True
    assert checkdata is not None
    assert checkdata.error ==  err


@pytest.mark.asyncio
@pytest.mark.usefixtures('fake_recordperiod')
async def test_countstore_measure_record_error():
    """Include failure in data sent via recordperiod channel"""
    measure = CountStore()
    noopcalled = False
    checkdata = None
    fail = stat.Failure(42)

    @stat.recordperiod
    async def check(data):
        nonlocal checkdata
        checkdata = data

    @measure(name='run')
    async def noop():
        """Do nothing"""
        nonlocal noopcalled
        noopcalled = True
        raise fail

    assert not measure

    with stat.recordperiod.open() as r:
        r.start()
        await noop()
        await stat.recordperiod.join()

    assert noopcalled is True
    assert checkdata is not None
    assert checkdata.error is None
    assert checkdata.failure == str(fail.args[0])


# ============================================================================
#
# ============================================================================
