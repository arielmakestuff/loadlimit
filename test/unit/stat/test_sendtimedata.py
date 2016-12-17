# -*- coding: utf-8 -*-
# test/unit/stat/test_sendtimedata.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test SendTimeData"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from copy import deepcopy

# Third-party imports
from pandas import to_timedelta
import pytest

# Local imports
import loadlimit.stat as stat
from loadlimit.stat import (CountStore, CountStoreData, DataChannel,
                            SendTimeData)
from loadlimit.util import now


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def fake_timedata(monkeypatch):
    fake_timedata = DataChannel(name='timedata')
    monkeypatch.setattr(stat, 'timedata', fake_timedata)


# ============================================================================
# Test init
# ============================================================================


def test_init_default_values():
    """Attrs get set to their default values"""
    c = CountStore()
    s = SendTimeData(c)

    assert s._countstore is c
    assert s._flushwait == 2
    assert s._channel is stat.timedata


@pytest.mark.parametrize('val', [42, 4.2, '42', [42]])
def test_init_bad_flushwait(val):
    """Raise error if flushwait arg is given a non-Timedelta value"""
    c = CountStore()
    expected = ('flushwait expected pandas.Timedelta, got {} instead'.
                format(type(val).__name__))

    with pytest.raises(TypeError) as err:
        SendTimeData(c, flushwait=val)

    assert err.value.args == (expected, )


@pytest.mark.parametrize('val', [42, 4.2, '42', [42]])
def test_init_bad_channel(val):
    """Raise error if channel arg is given a non-DataChannel value"""
    c = CountStore()
    expected = ('channel expected DataChannel, got {} instead'.
                format(type(val).__name__))

    with pytest.raises(TypeError) as err:
        SendTimeData(c, channel=val)

    assert err.value.args == (expected, )


# ============================================================================
# Test countdiff
# ============================================================================


@pytest.mark.asyncio
async def test_countdiff_no_prevcount():
    """Simply add the value if no prev count exists"""
    c = CountStore()
    s = SendTimeData(c)

    count = dict(hello=42)
    ret = await s.countdiff(count, {})
    assert ret == count
    assert ret is not count


@pytest.mark.asyncio
async def test_countdiff_single_prevcount():
    """Return single key diff"""
    c = CountStore()
    s = SendTimeData(c)

    count = dict(hello=42)
    prev = dict(hello=41)
    ret = await s.countdiff(count, prev)
    assert ret == dict(hello=1)


@pytest.mark.asyncio
async def test_countdiff_multi_prevcount():
    """Return multi key diff"""
    c = CountStore()
    s = SendTimeData(c)

    count = dict(hello=42, world=21, what=9001, now=40)
    prev = dict(hello=41, world=10, now=40)
    ret = await s.countdiff(count, prev)
    assert ret == dict(hello=1, world=11, what=9001, now=0)


# ============================================================================
# Test mkdata
# ============================================================================


@pytest.mark.asyncio
async def test_mkdata_no_prevcount():
    """Send correct rate if prevcount is None"""
    key = 'hello'
    c = CountStore()
    count = c[key]
    count.success += 1
    count.error['what'] += 1
    count.failure['now'] += 1
    s = SendTimeData(c)

    data = await s.mkdata(1, 42, key, c[key], None)

    assert isinstance(data, CountStoreData)
    assert data.name == key
    assert data.end == 42
    assert data.delta == 1
    assert data.rate == (count.success / 1)
    assert data.error == count.error
    assert data.failure == count.failure


@pytest.mark.asyncio
async def test_mkdata_zero_diff():
    """Diff of count and prevcount is 0 returns rate of 0"""
    key = 'hello'
    c = CountStore()
    c[key].success += 1
    count = c[key]
    s = SendTimeData(c)

    data = await s.mkdata(1, 42, key, count, count)

    assert data.rate == 0


@pytest.mark.asyncio
async def test_mkdata_diff():
    """Diff of count and prevcount returns correct rate"""
    key = 'hello'
    c = CountStore()
    c[key].success += 42
    count = c[key]
    prevcount = deepcopy(count)
    prevcount.success -= 10
    s = SendTimeData(c)

    data = await s.mkdata(2, 42, key, count, prevcount)

    assert data.rate == 5


@pytest.mark.asyncio
async def test_mkdata_noenddate():
    """If no end date yet exists, one is created"""
    key = 'hello'
    c = CountStore()
    c[key].success += 42
    count = c[key]
    prevcount = deepcopy(count)
    prevcount.success -= 10
    s = SendTimeData(c)

    data = await s.mkdata(2, None, key, count, prevcount)

    curdate = now()
    assert data.end is not None
    assert curdate >= data.end
    assert data.end.floor('D') == curdate.floor('D')


# ============================================================================
# Test send
# ============================================================================


@pytest.mark.usefixtures('fake_timedata')
@pytest.mark.asyncio
async def test_send_send():
    """Sends data through channel"""
    key = 'hello'
    c = CountStore()
    c.end_date = now()
    count = c[key]
    count.success += 1
    count.error['what'] += 1
    count.failure['now'] += 1
    channel = stat.timedata
    s = SendTimeData(c, channel=channel)
    checkdata = None

    @stat.timedata
    async def check(data):
        nonlocal checkdata
        checkdata = data

    with channel.open() as r:
        r.start()
        await s.send(1, c, None)
        await stat.timedata.join()

    curdate = now()
    assert isinstance(checkdata, CountStoreData)
    assert checkdata.name == key
    assert curdate >= checkdata.end
    assert checkdata.end.floor('D') == curdate.floor('D')


@pytest.mark.usefixtures('fake_timedata')
@pytest.mark.asyncio
async def test_send_diff():
    """Sends rate since previous rate"""
    key = 'hello'
    delta = 2

    prev = CountStore()
    prev.end_date = now()

    snap = deepcopy(prev)
    count = snap[key]
    count.success += 10
    count.error['what'] += 1
    count.failure['now'] += 1
    snap.end_date = now() + to_timedelta(delta, 's')
    channel = stat.timedata
    s = SendTimeData(snap, channel=channel)
    checkdata = None

    @channel
    async def check(data):
        nonlocal checkdata
        checkdata = data

    with channel.open() as r:
        r.start()
        await s.send(delta, snap, prev)
        await channel.join()

    assert isinstance(checkdata, CountStoreData)
    assert checkdata.name == key
    assert checkdata.end == snap.end_date
    assert round(checkdata.delta) == delta
    assert checkdata.rate == 5
    assert checkdata.error == count.error
    assert checkdata.failure == count.failure


# ============================================================================
# Test __call__
# ============================================================================


@pytest.mark.usefixtures('fake_timedata')
def test_call_stop_after_sleep(testloop):
    """Break out of loop if stop is True"""
    c = CountStore()
    channel = stat.timedata
    s = SendTimeData(c, channel=channel,
                     flushwait=to_timedelta(0.1, unit='s'))
    called = False
    calledcheck = False

    async def stopit():
        await s.shutdown()
        await channel.join()
        nonlocal called
        called = True

    @channel
    async def check(data):
        nonlocal calledcheck
        calledcheck = True

    corolist = [s(), stopit()]
    t = [asyncio.ensure_future(c) for c in corolist]
    f = asyncio.gather(*t)
    with channel.open() as r:
        r.start()
        testloop.run_until_complete(f)

    assert called is True
    assert s.stop is True
    assert calledcheck is False


@pytest.mark.usefixtures('fake_timedata')
def test_call_stop_after_send(testloop):
    """Break out of loop after send"""
    channel = stat.timedata
    called_send = False

    class Custom(SendTimeData):

        async def send(self, d, s, p):
            nonlocal called_send
            called_send = True
            await super().send(d, s, p)
            await channel.join()

    key = 'hello'
    c = CountStore()
    c.end_date = now()
    count = c[key]
    count.success += 1
    count.error['what'] += 1
    count.failure['now'] += 1
    s = Custom(c, channel=channel, flushwait=to_timedelta(0.1, unit='s'))
    calledcheck = False

    @channel
    async def check(data):
        nonlocal calledcheck
        calledcheck = True
        await s.shutdown()

    t = asyncio.ensure_future(s())
    with channel.open() as r:
        r.start()
        testloop.run_until_complete(t)

    assert calledcheck is True
    assert s.stop is True


@pytest.mark.usefixtures('fake_timedata')
def test_call_stop_after_send_aftersleep(testloop):
    """Break out of loop after second sleep"""
    channel = stat.timedata
    called_send = False

    class Custom(SendTimeData):

        _calledcount = 0

        async def send(self, d, s, p):
            nonlocal called_send
            called_send = True
            await super().send(d, s, p)
            if self._calledcount == 1:
                await self.shutdown()
                return
            self._calledcount += 1

    key = 'hello'
    c = CountStore()
    c.end_date = now()
    count = c[key]
    count.success += 1
    count.error['what'] += 1
    count.failure['now'] += 1
    s = Custom(c, channel=channel, flushwait=to_timedelta(0, unit='s'))

    t = asyncio.ensure_future(s())
    with channel.open() as r:
        r.start()
        testloop.run_until_complete(t)

    assert called_send is True
    assert s.stop is True
    assert s._calledcount == 1


@pytest.mark.usefixtures('fake_timedata')
def test_call_send(testloop):
    """Store diff"""
    channel = stat.timedata
    called_send = False

    class Custom(SendTimeData):

        async def send(self, d, s, p):
            nonlocal called_send
            called_send = True
            await super().send(d, s, p)
            await self.shutdown()
            await channel.join()

    key = 'hello'
    c = CountStore()
    channel = stat.timedata
    s = Custom(c, channel=channel, flushwait=to_timedelta(0.1, unit='s'))
    called = False
    checkdata = None

    async def stopit():
        nonlocal called
        called = True
        c.end_date = now()
        count = c[key]
        count.success += 1
        count.error['what'] += 1
        count.failure['now'] += 1

    @channel
    async def check(data):
        nonlocal checkdata
        checkdata = data

    corolist = [s(), stopit()]
    t = [asyncio.ensure_future(c) for c in corolist]
    f = asyncio.gather(*t)
    with channel.open() as r:
        r.start()
        testloop.run_until_complete(f)

    assert called is True
    assert called_send is True
    assert s.stop is True
    assert checkdata is not None

    curdate = now()
    assert isinstance(checkdata, CountStoreData)
    assert checkdata.name == key
    assert checkdata.end <= curdate
    assert checkdata.end.floor('D') == curdate.floor('D')
    assert round(checkdata.delta, 1) == 0.1
    assert round(checkdata.rate) == 1 / 0.1
    assert checkdata.error == dict(what=1)
    assert checkdata.failure == dict(now=1)


# ============================================================================
#
# ============================================================================
