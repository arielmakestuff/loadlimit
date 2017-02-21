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
import asyncio
from asyncio import ensure_future, Event, gather, Queue, sleep
from collections import defaultdict, OrderedDict
from collections.abc import MutableMapping
from contextlib import contextmanager
from enum import Enum
from functools import partial
from itertools import count

# Third-party imports

# Local imports
from .util import ageniter, Logger


# ============================================================================
# Globals
# ============================================================================


ChannelState = Enum('ChannelState', ['closed', 'open', 'listening', 'paused',
                                     'closing'])
ChannelCommand = Enum('ChannelCommand', ['start', 'stop', 'pause'])
AnchorType = Enum('AnchorType', ['none', 'first', 'last'])


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

    def open(self, *, loop=None, cleanup=None, **kwargs):
        """Open the data channel

        ChannelOpenError is raised if the channel is already open.

        """
        loop = asyncio.get_event_loop() if loop is None else loop
        kwargs['loop'] = loop
        parent = self._parent
        openstates = (ChannelState.open, ChannelState.listening,
                      ChannelState.paused)
        if parent._state in openstates:
            raise ChannelOpenError
        parent._qkwargs = kwargs
        parent._queue = parent._qcls(**kwargs)

        # Setup cleanup notification channel
        if cleanup is not None and not isinstance(cleanup, DataChannel):
            msg = ('cleanup expected DataChannel object, got {} instead'.
                   format(type(cleanup).__name__))
            raise TypeError(msg)
        parent._cleanup = cleanup

        # Setup openqueue event
        oq = parent._openqueue
        if oq is None:
            parent._openqueue = oq = Event(loop=loop)
        oq.set()

        parent._command = Queue(maxsize=1, loop=loop)
        parent._state = ChannelState.open
        return self

    def close(self):
        """Close the channel"""
        parent = self._parent
        state = parent._state
        if state == ChannelState.closed:
            return
        elif state not in(ChannelState.open, ChannelState.closing):
            parent.stop()
        parent._openqueue.clear()
        parent._cleanup = None
        parent._queue = None
        parent._command = None
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

    def __init__(self, *, queuecls=None, name=None, logger=None):
        self._qcls = Queue if queuecls is None else queuecls
        self._tasks = defaultdict(OrderedDict)
        self._qkwargs = None
        self._queue = None
        self._openqueue = None
        self._command = None
        self._availkeys = set()
        self._keygen = count()
        self._state = ChannelState.closed
        self._cleanup = None
        self._name = 'datachannel' if name is None else name
        self._logger = l = Logger(logger=logger)
        l.msgkwargs.update(name=name)

    def __call__(self, corofunc=None, *, anchortype=None, keyobj=None,
                 **kwargs):
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
        for tdict in self._tasks.values():
            for k in tdict:
                yield k

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
        self.logger.debug('channel {name}: open')
        return self.channel.open(**kwargs)

    def close(self):
        """Close the channel"""
        self.logger.debug('channel {name}: close')
        self.channel.close()

    def add(self, corofunc, *, anchortype=None):
        """Add a coroutine function listener"""
        self.logger.debug('channel {name}: adding corofunc {!r}', corofunc)
        if not callable(corofunc):
            msg = ('corofunc expected callable, got {} instead'.
                   format(type(corofunc).__name__))
            raise TypeError(msg)

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
        async for anchortype, tdict in ageniter(self._tasks.items()):
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
        self.logger.debug('channel {name}: start listener')
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
        else:
            loop = asyncio.get_event_loop()
            loop.call_soon_threadsafe(self._command.put_nowait,
                                      ChannelCommand.start)

    def stop(self):
        """Stop scheduling listeners"""
        self.logger.debug('channel {name}: stop listener')
        state = self._state
        if state == ChannelState.closed:
            raise ChannelClosedError
        elif state == ChannelState.open:
            return
        elif state != ChannelState.closing:
            self._state = ChannelState.open
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(self._command.put_nowait,
                                  ChannelCommand.stop)

    def pause(self):
        """Pause scheduling listeners"""
        self.logger.debug('channel {name}: pause listener')
        state = self._state
        if state == ChannelState.closed:
            raise ChannelClosedError
        elif state == ChannelState.open:
            raise NotListeningError
        self._state = ChannelState.paused
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(self._command.put_nowait,
                                  ChannelCommand.pause)

    async def _listen(self, kwargs, *, asyncfunc=True, loop=None):
        """Listen for data on the queue"""
        queue = self._queue
        command = self._command
        task_done = queue.task_done
        tasks = self._tasks
        runfunc = self._aruntasks if asyncfunc else self._runtasks
        data = None
        cmd = None
        checkcmd = False
        cleanup = None
        while True:
            # Check for commands
            if checkcmd or not command.empty():
                cmd = await command.get()
                checkcmd = False
            if cmd is not None:
                command.task_done()
                if cmd == ChannelCommand.stop:
                    if queue.qsize() == 0:
                        break

                    # Setup cleanup channel
                    cleanup = self._cleanup
                    if (cleanup is not None and
                            cleanup.state == ChannelState.listening):
                        await cleanup.send(queue.qsize())
                elif cmd == ChannelCommand.pause:
                    checkcmd = True
                    await sleep(0)
                    continue
                cmd = None
            if not queue.empty():
                data = await queue.get()
                ensure_future(runfunc(data, tasks, task_done, kwargs,
                                      loop=loop), loop=loop)

                # Send current qsize to cleanup channel
                if (cleanup is not None and
                        cleanup.state == ChannelState.listening):
                    await cleanup.send(queue.qsize())
            await sleep(0)

    async def _runtasks(self, data, tasks, task_done, kwargs, loop=None):
        """Run listeners syncronously"""
        order = [AnchorType.first, AnchorType.none, AnchorType.last]

        async for anchortype in ageniter(order):
            g = tasks[anchortype].values()
            generator = (reversed(g) if anchortype == AnchorType.last else g)
            async for corofunc in ageniter(generator):
                await corofunc(data, **kwargs)
                await sleep(0)
            await sleep(0)
        task_done()

    async def _aruntasks(self, data, tasks, task_done, kwargs, loop=None):
        """Schedule listeners"""
        order = [AnchorType.first, AnchorType.none, AnchorType.last]

        async for anchortype in ageniter(order):
            t = [ensure_future(corofunc(data, **kwargs), loop=loop)
                 for corofunc in tasks[anchortype].values()]
            if t:
                await gather(*t, loop=loop)
            await sleep(0)
        task_done()

    async def send(self, data):
        """Send data into the channel

        If the channel hasn't been opened yet, blocks until the channel is
        opened.

        """
        while True:
            openqueue = self._openqueue
            if openqueue is None:
                await sleep(0)
                continue
            await openqueue.wait()
            break
        await self._queue.put(data)

    def put(self, data):
        """Put data immediately into the channel

        If the channel hasn't been opened yet, ChannelClosedError is raised.

        """
        if self._state == ChannelState.closed:
            raise ChannelClosedError
        self._queue.put_nowait(data)

    async def join(self):
        """Block until work queue is done"""
        queue = self._queue
        if queue is None:
            return
        await queue.join()

    async def shutdown(self, *args, **kwargs):
        """Shutdown and wait for listener to be cleared from the event loop

        This will close the channel.

        """
        self.logger.debug('channel {name}: shutdown')
        if self._state == ChannelState.closed:
            return
        self._openqueue.clear()
        self._state = ChannelState.closing
        self.stop()
        await self.join()
        self.close()

    # --------------------
    # Descriptors
    # --------------------

    @property
    def logger(self):
        """Return the channel's logger"""
        return self._logger

    @property
    def name(self):
        """Return the channel's name"""
        return self._name

    @property
    def state(self):
        """Return current channel state"""
        return self._state

    channel = ChannelContext()


# ============================================================================
# CommandChannel
# ============================================================================


class CommandChannel(DataChannel):

    def __init__(self, cmdenum, *, queuecls=None, name='commandchannel'):
        if not isinstance(cmdenum, type) or not issubclass(cmdenum, Enum):
            msg = 'cmdenum expected Enum subclass, got {}'
            raise TypeError(msg.format(type(cmdenum).__name__))
        elif not hasattr(cmdenum, 'none'):
            msg = "'none' member of {} enum not found"
            raise ValueError(msg.format(cmdenum.__name__))
        super().__init__(queuecls=queuecls, name=name)
        self._enum = cmdenum
        self._qcls = partial(self._qcls, 1)
        self._tasks = defaultdict(partial(defaultdict, OrderedDict))

    def __getitem__(self, key):
        switch_tasks = self._switch_tasks
        for command in self._tasks:
            with switch_tasks(command):
                try:
                    ret = super().__getitem__(key)
                except KeyError:
                    pass
                else:
                    return ret
        raise KeyError(key)

    def __iter__(self):
        switch_tasks = self._switch_tasks
        for command in self._tasks:
            with switch_tasks(command):
                for k in super().__iter__():
                    yield k

    def __contains__(self, key):
        switch_tasks = self._switch_tasks
        for command in self._tasks:
            with switch_tasks(command):
                ret = super().__contains__(key)
                if ret:
                    return True
        return False

    def __len__(self):
        ret = 0
        switch_tasks = self._switch_tasks
        for command in self._tasks:
            with switch_tasks(command):
                ret = ret + super().__len__()
        return ret

    @contextmanager
    def _switch_tasks(self, command):
        """switch_tasks"""
        root = self._tasks
        self._tasks = t = root[command]
        try:
            yield t
        finally:
            self._tasks = root

    def anchortype(self, key):
        """Return the anchortype associated with the key"""
        switch_tasks = self._switch_tasks
        for command in self._tasks:
            with switch_tasks(command):
                try:
                    ret = super().anchortype(key)
                except KeyError:
                    pass
                else:
                    return ret
        raise KeyError(key)

    def findcommand(self, key):
        """Return the command the key is subscribed to"""
        switch_tasks = self._switch_tasks
        for command in self._tasks:
            with switch_tasks(command):
                try:
                    super().__getitem__(key)
                except KeyError:
                    pass
                else:
                    return command
        raise KeyError(key)

    def add(self, corofunc, *, anchortype=None, command=None):
        """Add a coroutine function listener to the given command"""
        enum = self._enum
        if command is None:
            command = enum.none
        if not isinstance(command, enum):
            msg = 'command expected {}, got {} instead'
            raise TypeError(msg.format(enum.__name__, type(command).__name__))

        with self._switch_tasks(command):
            super().add(corofunc, anchortype=anchortype)

    def remove(self, key):
        """Remove the added coro func with the given key"""
        switch_tasks = self._switch_tasks
        for command in self._tasks:
            with switch_tasks(command):
                try:
                    super().remove(key)
                except KeyError:
                    pass
                else:
                    return
        raise KeyError(key)

    async def aremove(self, key):
        """Asynchronously remove the added coro func with the given key"""
        switch_tasks = self._switch_tasks
        async for command in ageniter(self._tasks):
            with switch_tasks(command):
                try:
                    await super().aremove(key)
                except KeyError:
                    await sleep(0)
                else:
                    return
        raise KeyError(key)

    async def _runtasks(self, cmd_data, tasks, task_done, kwargs, loop=None):
        """Run listeners syncronously"""
        command, data = cmd_data
        if command not in tasks:
            task_done()
            return

        await super()._runtasks(data, tasks[command], task_done, kwargs,
                                loop=loop)

    async def _aruntasks(self, cmd_data, tasks, task_done, kwargs, loop=None):
        """Schedule listeners"""
        command, data = cmd_data
        if command not in tasks:
            task_done()
            return

        await super()._aruntasks(data, tasks[command], task_done, kwargs,
                                 loop=loop)

    async def send(self, command, data=None):
        """Send command into the channel"""
        enum = self._enum
        if not isinstance(command, enum):
            msg = 'command expected {}, got {} instead'
            raise TypeError(msg.format(enum.__name__, type(command).__name__))
        await super().send((command, data))

    def put(self, command, data=None):
        """Put data immediately into the channel"""
        enum = self._enum
        if not isinstance(command, enum):
            msg = 'command expected {}, got {} instead'
            raise TypeError(msg.format(enum.__name__, type(command).__name__))
        super().put((command, data))


# ============================================================================
# Shutdown channel
# ============================================================================


ShutdownCommand = Enum('ShutdownCommand', ['none', 'start'])


class ShutdownChannel(CommandChannel):
    """Shutdown commands"""

    def __init__(self):
        super().__init__(ShutdownCommand, name='shutdown')

    def add(self, corofunc, *, anchortype=None):
        """Add a coroutine function listener to the given command"""
        return super().add(corofunc, anchortype=anchortype,
                           command=ShutdownCommand.start)

    def start(self, *, loop=None, **kwargs):
        """Start listeners"""
        super().start(loop=loop, asyncfunc=False, **kwargs)

    async def send(self, data=None):
        """Send command into the channel"""
        await super().send(ShutdownCommand.start, data)

    def put(self, data=None):
        """Put command immediately into the channel"""
        if self._queue.full():
            msg = ('Not sending shutdown({}): shutdown already requested'.
                   format(data))
            self.logger.warning(msg)
        else:
            super().put(ShutdownCommand.start, data)


shutdown = ShutdownChannel()


# ============================================================================
#
# ============================================================================
