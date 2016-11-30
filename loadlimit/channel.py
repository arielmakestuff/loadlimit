# -*- coding: utf-8 -*-
# loadlimit/channel.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Define data channels"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from asyncio import ensure_future, gather, Queue, sleep
from collections import defaultdict, OrderedDict
from collections.abc import MutableMapping
from enum import Enum
from itertools import count

# Third-party imports

# Local imports
from .util import aiter


# ============================================================================
# Globals
# ============================================================================


ChannelState = Enum('ChannelState', ['closed', 'open', 'listening', 'paused'])
AnchorType = Enum('AnchorType', ['none', 'first', 'last'])


class Command(Enum):
    """Base enum for commands"""
    pass


# ============================================================================
# Exceptions
# ============================================================================


class ChannelError(Exception):
    """Base class for all channel exceptions"""


class ChannelOpenError(ChannelError):
    """Exception raised when trying to open an already opened channel"""

class ChannelListeningError(ChannelError):
    """Exception raised when trying listen to an already listening channel"""

class ChannelClosedError(ChannelError):
    """Exception for trying to operate on a closed channel"""

class NotListeningError(ChannelError):
    """Exception for trying to continue listening when nothing is listnineg"""


# ============================================================================
# ChannelContext
# ============================================================================


class ChannelContext:
    """Context manager that instantiates an asyncio.Queue"""

    def __init__(self, parent=None):
        self._parent = parent

    def __call__(self, **kwargs):
        parent = self._parent
        parent._qkwargs = kwargs
        return self

    def __get__(self, obj, objtype):
        return self.__class__(parent=obj)

    def __enter__(self):
        parent = self._parent
        if parent._state != ChannelState.open:
            kwargs = parent._qkwargs
            if kwargs is None:
                kwargs = {}
            self.open(**kwargs)
        return parent

    def __exit__(self, errtype, err, errtb):
        self.close()

    def open(self, **kwargs):
        """Open the data channel

        ChannelOpenError is raised if the channel is already open.

        """
        parent = self._parent
        openstates = (ChannelState.open, ChannelState.listening,
                      ChannelState.paused)
        if parent._state in openstates:
            raise ChannelOpenError
        parent._qkwargs = kwargs
        parent._queue = parent._qcls(**kwargs)
        parent._state = ChannelState.open
        return self

    def close(self):
        """Close the channel"""
        parent = self._parent
        state = parent._state
        if state == ChannelState.closed:
            return
        parent._queue = None
        parent._qkwargs = None
        parent._state = ChannelState.closed


# ============================================================================
# DataChannel
# ============================================================================


class DataChannel:
    """Base data channel"""

    # --------------------
    # Methods
    # --------------------

    def __init__(self, *, queuecls=None):
        self._qcls = Queue if queuecls is None else queuecls
        self._tasks = defaultdict(OrderedDict)
        self._qkwargs = None
        self._queue = None
        self._availkeys = set()
        self._keygen = count()
        self._state = ChannelState.closed

    def __call__(self, corofunc=None, anchortype=None, keyobj=None, **kwargs):
        """Decorator to add a corofunc listener to the channel"""

        def addcoro(corofunc):
            """Add corofunc with kwargs"""
            key = self.add(corofunc, anchortype=anchortype, **kwargs)
            if keyobj is not None:
                if isinstance(keyobj, MutableMapping):
                    keyobj['key'] = key
                else:
                    keyobj.key = key
            return corofunc

        if corofunc is None:
            return addcoro

        return addcoro(corofunc)

    def __getitem__(self, key):
        for _, tdict in self._tasks.items():
            for k, func in tdict.items():
                if k == key:
                    return func
        raise KeyError(key)

    def __iter__(self):
        keys = {k for a, tdict in self._tasks.items()
                for k in tdict}
        return iter(keys)

    def __contains__(self, key):
        for tdict in self._tasks.values():
            if key in tdict:
                return True
        return False

    def __delitem__(self, key):
        self.remove(key)

    def __len__(self):
        return sum(len(d) for d in self._tasks.values())

    def anchortype(self, key):
        """Return the anchortype associated with the key"""
        for atype, tdict in self._tasks.items():
            for k in tdict:
                if k == key:
                    return atype
        raise KeyError(key)

    def open(self, **kwargs):
        """Open the data channel"""
        return self.channel.open(**kwargs)

    def close(self):
        """Close the channel"""
        self.channel.close()

    def add(self, corofunc, anchortype=None):
        """Add a coroutine function listener"""
        if not callable(corofunc):
            msg = 'corofunc expected callable, got {} instead'
            raise TypeError(msg.format(type(corofunc).__name__))

        avail = self._availkeys
        tasks = self._tasks
        anchortype = AnchorType.none if anchortype is None else anchortype
        if not isinstance(anchortype, AnchorType):
            msg = 'anchortype expected AnchorType, got {} instead'
            raise TypeError(msg.format(type(anchortype).__name__))
        key = avail.pop() if avail else next(self._keygen)
        tasks[anchortype][key] = corofunc
        return key

    def remove(self, key):
        """Remove the added coro func with the given key"""
        for anchortype, tdict in self._tasks.items():
            if key in tdict:
                tdict.__delitem__(key)
                break
        else:
            raise KeyError(key)
        self._availkeys.add(key)

    async def aremove(self, key):
        """Asynchronously remove the added coro func with the given key"""
        async for anchortype, tdict in aiter(self._tasks.items()):
            if key in tdict:
                tdict.__delitem__(key)
                break
        else:
            raise KeyError(key)
        await sleep(0)
        self._availkeys.add(key)

    def clear(self):
        """Remove all added coro funcs"""
        self._availkeys.clear()
        self._tasks.clear()

    def isopen(self):
        """Return True if the channel is open"""
        return self._state in (ChannelState.open, ChannelState.listening,
                               ChannelState.paused)

    def start(self, *, loop=None, asyncfunc=True, **kwargs):
        """Start listeners"""
        state = self._state
        if state == ChannelState.closed:
            raise ChannelClosedError
        elif state == ChannelState.listening:
            raise ChannelListeningError

        oldstate = self._state
        self._state = ChannelState.listening
        if oldstate == ChannelState.open:
            coro = self._listen(kwargs, asyncfunc=asyncfunc, loop=loop)
            ensure_future(coro, loop=loop)

    def stop(self):
        """Stop scheduling listeners"""
        state = self._state
        if state == ChannelState.closed:
            raise ChannelClosedError
        elif state == ChannelState.open:
            raise NotListeningError
        self._state = ChannelState.open

    def pause(self):
        """Pause scheduling listeners"""
        state = self._state
        if state == ChannelState.closed:
            raise ChannelClosedError
        elif state == ChannelState.open:
            raise NotListeningError
        self._state = ChannelState.paused

    async def _listen(self, kwargs, *, asyncfunc=True, loop=None):
        """Listen for data on the queue"""
        queue = self._queue
        tasks = self._tasks
        runfunc = self._aruntasks if asyncfunc else self._runtasks
        while True:
            state = self._state
            if state in (ChannelState.closed, ChannelState.open):
                break
            elif state != ChannelState.listening:
                await sleep(0)
                continue
            data = await queue.get()
            ensure_future(runfunc(data, tasks, kwargs, loop=loop), loop=loop)
            await sleep(0)

    async def _runtasks(self, data, tasks, kwargs, loop=None):
        """Run listeners syncronously"""
        order = [AnchorType.first, AnchorType.none, AnchorType.last]

        async for anchortype in aiter(order):
            g = tasks[anchortype].values()
            generator = (reversed(g) if anchortype == AnchorType.last else g)
            async for corofunc in aiter(generator):
                await corofunc(data, **kwargs)
                await sleep(0)

    async def _aruntasks(self, data, tasks, kwargs, loop=None):
        """Schedule listeners"""
        order = [AnchorType.first, AnchorType.none, AnchorType.last]

        async for anchortype in aiter(order):
            t = [ensure_future(corofunc(data, **kwargs), loop=loop)
                 for corofunc in tasks[anchortype].values()]
            if t:
                await gather(*t, loop=loop)
            await sleep(0)

    async def send(self, **kwargs):
        """Send data into the channel

        If the channel hasn't been opened yet, blocks until the channel is
        opened.

        """
        while True:
            if self._state == ChannelState.closed:
                await sleep(0)
                continue
            break
        await self._queue.put(kwargs)

    def put(self, **kwargs):
        """Put data immediately into the channel

        If the channel hasn't been opened yet, ChannelClosedError is raised.

        """
        if self._state == ChannelState.closed:
            raise ChannelClosedError
        self._queue.put_nowait(kwargs)

    # --------------------
    # Descriptors
    # --------------------

    @property
    def state(self):
        """Return current channel state"""
        return self._state

    channel = ChannelContext()


# ============================================================================
#
# ============================================================================
