# -*- coding: utf-8 -*-
# loadlimit/event.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Define asyncio events"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from asyncio import Event, ensure_future, iscoroutinefunction
from enum import Enum
from functools import wraps

# Third-party imports

# Local imports
from .util import Namespace


# ============================================================================
# Globals
# ============================================================================


AnchorType = Enum('AnchorType', ['first', 'last'])


# ============================================================================
# Exceptions
# ============================================================================


class EventError(Exception):
    """Base class for all event exceptions"""


class EventNotStartedError(EventError):
    """Exception raised when event methods called but event not yet started"""


class NoEventTasksError(EventError):
    """Exception raised when event methods called but event not yet started"""


# ============================================================================
# Helpers
# ============================================================================


def event_started(meth):
    """Decorator to check if the event has started already."""

    @wraps(meth)
    def check_event(self, *args, **kwargs):
        """Check event started"""
        event = self._event
        if event is None:
            raise EventNotStartedError

        return meth(self, *args, **kwargs)

    return check_event


# ============================================================================
# BaseEvent
# ============================================================================


class LoadLimitEvent:
    """Base class of all asyncio loadlimit events"""

    def __init__(self):
        self._event = None
        self._kwargs = {}
        self._tasks = set()
        self._waiting = set()

    def __iter__(self):
        """Iterate over all tasks waiting for the event"""
        return iter(self._waiting)

    def __call__(self, corofunc=None):
        """Decorator to add a corofunc to the event"""

        def addcoro(corofunc):
            """Add corofunc with kwargs"""
            self.add(corofunc)
            return corofunc

        if corofunc is None:
            return addcoro

        return addcoro(corofunc)

    @event_started
    def clear(self):
        """Reset internal flag to False.

        Essentially stops the event.

        """
        event = self._event
        event.clear()

        # Cancel tasks already waiting for the event
        waiting = {t for t in self._waiting if not t.done()}
        if waiting:
            tasks_future = asyncio.gather(*waiting)
            tasks_future.cancel()
        self._waiting.clear()

        self._event = None
        if not self._kwargs.done():
            self._kwargs.cancel()
        self._kwargs = None

    @event_started
    def is_set(self):
        """Return True iff internal flag is True"""
        return self._event.is_set()

    @event_started
    def set(self, **kwargs):
        """Set internal flag to true.

        This will also set the event's result to any given kwargs.

        """
        self._kwargs.set_result(kwargs)
        self._event.set()

    async def wait(self):
        """Block until the internal flag is True

        Raises EventNotStartedError if called before the event has started.

        """
        event = self._event
        if event is None:
            raise EventNotStartedError
        await event.wait()

    def add(self, *tasks):
        """Add one or more tasks for the event"""

        def itertasks(alltasks):
            """Verify each task is a coro func"""
            for t in alltasks:
                if not iscoroutinefunction(t):
                    msg = 'tasks expected coroutine, got {} instead'
                    raise TypeError(msg.format(type(t).__name__))
                yield t

        self._tasks.update(itertasks(tasks))

    def start(self, *, loop=None, **kwargs):
        """Start the event.

        Schedule all tasks not yet waiting for the event.

        This will also re-set the internal flag to False and create a brand new
        future for the event.

        """
        event = self._event
        tasks = self._tasks
        if event:
            self.clear()

        # If no tasks, raise an error
        if not tasks:
            raise NoEventTasksError

        # Create a new event
        event, result = self._create_event(loop)

        # Schedule all tasks
        self._schedule_tasks(tasks, result, kwargs, loop)

    def _create_event(self, loop):
        """Create a new event"""
        if loop is None:
            loop = asyncio.get_event_loop()
        self._kwargs = f = loop.create_future()
        self._event = event = Event(loop=loop)
        return (event, f)

    def _schedule_tasks(self, tasks, result, kwargs, loop):
        """Schedule tasks"""
        # Schedule all tasks
        runtask = self.runtask
        self._waiting.update(
            ensure_future(runtask(corofunc, result, kwargs), loop=loop)
            for corofunc in tasks)
        tasks.clear()

    async def runtask(self, corofunc, future, kwargs):
        """Wait for the event and then run the task"""
        await self.wait()
        result = Namespace(**future.result())
        await corofunc(result, **kwargs)

    @property
    def waiting(self):
        """Return set of all tasks waiting for the event"""
        return frozenset(self._waiting)

    @property
    def tasks(self):
        """Return set of tasks not yet waiting for the event"""
        return frozenset(self._tasks)

    @property
    def started(self):
        """Return True if the event has started"""
        return self._event is not None


# ============================================================================
# Anchor
# ============================================================================


class Anchor(LoadLimitEvent):
    """Define a coro func that will run anchor all other tasks"""

    def __init__(self):
        super().__init__()
        self._anchortype = None
        self._anchorfunc = (None, None)
        self._anchortask = None

    def __call__(self, corofunc=None, anchortype=None):
        """Decorator to add a corofunc to the event"""
        self.anchortype = anchortype
        return super().__call__(corofunc)

    def add(self, *tasks):
        """Adds tasks

        Prevents anchorfunc from being added as a normal task

        """
        super().add(*tasks)

        # Set anchorfunc
        anchortype = self._anchortype
        if anchortype:
            self._anchorfunc = (anchortype, tasks[0])
            self._anchortype = None

    def _schedule_tasks(self, tasks, result, kwargs, loop):
        """Schedule all tasks

        lastfunc is not included as a normal task and is scheduled separately
        using the runlast coro.

        """
        anchortype, anchorfunc = self._anchorfunc
        if anchorfunc is None:
            super()._schedule_tasks(tasks, result, kwargs, loop)
        else:
            self._waiting = waiting = {t for t in tasks if t != anchorfunc}
            self._anchortask = ensure_future(
                self.anchor(anchorfunc, anchortype, waiting, result,
                            kwargs, loop),
                loop=loop)
            tasks.clear()
            tasks.add(anchorfunc)

    async def anchor(self, corofunc, anchortype, tasks, future, kwargs, loop):
        """Wait for the event and all other tasks before running corofunc"""
        await self.wait()
        result = future.result()

        # Run the first coro
        if anchortype == AnchorType.first:
            await corofunc(Namespace(**result), **kwargs)

        # schedule all tasks
        if tasks:
            self._waiting = set()
            super()._schedule_tasks(tasks, future, kwargs, loop)

        # Wait for all other tasks to finish
        waiting = self._waiting
        if waiting:
            await asyncio.gather(*waiting, loop=loop)

        # Run the last coro
        if anchortype == AnchorType.last:
            await corofunc(Namespace(**result), **kwargs)

    @property
    def anchortype(self):
        """Get current anchortype"""
        return self._anchortype

    @anchortype.setter
    def anchortype(self, val):
        """Set the value of the anchortype"""
        if val is not None and not isinstance(val, AnchorType):
            msg = 'anchortype expected AnchorType or NoneType, got {} instead'
            raise TypeError(msg.format(type(val).__name__))
        self._anchortype = val

    @property
    def anchorfunc(self):
        """Get current anchor function"""
        return self._anchorfunc[1]

    @property
    def anchortask(self):
        """Get anchor task"""
        return self._anchortask


# ============================================================================
# RunLast
# ============================================================================


class RunLast(Anchor):
    """Define a coro func that will run after all other tasks"""

    def __call__(self, corofunc=None, runlast=False):
        """Decorator to add a corofunc to the event"""
        anchortype = AnchorType.last if runlast else None
        return super().__call__(corofunc, anchortype=anchortype)


# ============================================================================
# RunFirst
# ============================================================================


class RunFirst(Anchor):
    """Define a coro func that will run before all other tasks"""

    def __call__(self, corofunc=None, runfirst=False):
        """Decorator to add a corofunc to the event"""
        anchortype = AnchorType.first if runfirst else None
        return super().__call__(corofunc, anchortype=anchortype)


# ============================================================================
# Events
# ============================================================================


shutdown = RunLast()


# ============================================================================
#
# ============================================================================
