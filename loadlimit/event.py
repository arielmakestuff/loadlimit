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
from asyncio import Event, ensure_future
from collections import defaultdict
from enum import Enum
from functools import wraps

# Third-party imports

# Local imports
from .util import ageniter, Namespace


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
        self._result = None
        self._tasks = set()
        self._waiting = set()
        self._option = None
        self._cb = []

    def __iter__(self):
        """Iterate over all tasks waiting for the event"""
        return iter(self._waiting)

    def __call__(self, corofunc=None, schedule=True, **kwargs):
        """Decorator to add a corofunc to the event"""

        def addcoro(corofunc):
            """Add corofunc with kwargs"""
            self.add(corofunc, schedule=schedule, **kwargs)
            return corofunc

        if corofunc is None:
            return addcoro

        return addcoro(corofunc)

    @event_started
    def stop(self):
        """Stop the event

        This calls clear() first before stopping the event.

        """
        self.clear()

        # Cancel tasks already waiting for the event
        waiting = {t for t in self._waiting if not t.done()}
        if waiting:
            tasks_future = asyncio.gather(*waiting)
            tasks_future.cancel()
        self._waiting.clear()

        self._event = None
        self._result.cancel()
        self._result = None

    @event_started
    def clear(self):
        """Reset internal flag to False.

        This will also create a brand new future if the current future is not
        done.

        """
        self._event.clear()
        if self._result.done():
            loop = asyncio.get_event_loop()
            self._result = loop.create_future()

    @event_started
    def is_set(self):
        """Return True iff internal flag is True"""
        return self._event.is_set()

    @event_started
    def set(self, *, callclear=False, **kwargs):
        """Set internal flag to true.

        This will also set the event's result to any given kwargs.

        """
        self._result.set_result(kwargs)
        self._event.set()
        if callclear:
            self.clear()

    async def wait(self):
        """Block until the internal flag is True

        Raises EventNotStartedError if called before the event has started.

        """
        event = self._event
        if event is None:
            raise EventNotStartedError
        await event.wait()

    def add(self, *tasks, schedule=True, **kwargs):
        """Add one or more tasks for the event"""

        def itertasks(alltasks):
            """Verify each task is a coro func"""
            for t in alltasks:
                if not callable(t):
                    msg = 'tasks expected callable, got {} instead'
                    raise TypeError(msg.format(type(t).__name__))
                yield t

        if schedule:
            self._tasks.update(itertasks(tasks))
        else:
            self._cb.extend(itertasks(tasks))

    def start(self, *, loop=None, reschedule=False, **kwargs):
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
        if not tasks and not self._cb:
            raise NoEventTasksError

        # Create a new event
        self._create_event(loop)

        # Schedule all tasks
        self._option = Namespace(reschedule=reschedule)
        self._schedule_tasks(tasks, kwargs, loop)

    def _create_event(self, loop):
        """Create a new event"""
        if loop is None:
            loop = asyncio.get_event_loop()
        self._result = loop.create_future()
        self._event = Event(loop=loop)

    def _schedule_tasks(self, tasks, kwargs, loop):
        """Schedule tasks"""
        if self._cb:
            ensure_future(self.runcb(tasks, kwargs, loop), loop=loop)
        else:
            runtask = self.runtask
            self._waiting.update(
                ensure_future(runtask(corofunc, kwargs), loop=loop)
                for corofunc in tasks)

    async def runtask(self, corofunc, kwargs):
        """Wait for the event and then run the task"""
        while True:
            future = self._result
            await self.wait()
            result = Namespace(**future.result())
            await corofunc(result, **kwargs)

            if not self._option.reschedule:
                break

    async def runcb(self, tasks, kwargs, loop):
        """Wait for the event and then run all non-scheduled coro funcs"""
        while True:
            future = self._result
            await self.wait()
            result = Namespace(**future.result())

            # Run non-scheduled coro funcs
            async for corofunc in ageniter(self._cb):
                await corofunc(result, **kwargs)

            # schedule all tasks
            if tasks:
                self._waiting = waiting = {
                    ensure_future(corofunc(result, **kwargs), loop=loop)
                    for corofunc in tasks
                }

            # Wait for all other tasks to finish
            waiting = self._waiting
            if waiting:
                await asyncio.gather(*waiting, loop=loop)

            waiting.clear()

            if not self._option.reschedule:
                break

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

    @property
    def option(self):
        """Return the container option Namespace"""
        return self._option

    @property
    def noschedule(self):
        """Return a list of coro funcs that will not be scheduled

        Tasks are coro funcs that are scheduled to run. This set of coro funcs,
        however, are run directly in relation to the anchor.

        """
        return frozenset(self._cb)


# ============================================================================
# MultiEvent
# ============================================================================


class MultiEvent:
    """Event container

    Mimicks the LoadLimitEvent interface with some modifications (chiefly
    providing the eventid to the method in order to use the correct event).

    """

    def __init__(self, default_factory=None):
        if default_factory is None:
            default_factory = LoadLimitEvent
        default_factory = self.validate_factory(default_factory)
        self._event = defaultdict(default_factory)
        self._loop = None
        self._pending_call = []

    def __iter__(self):
        """Iterate over all eventids"""
        return iter(self._event)

    def __setitem__(self, eventid, val):
        """Set the event for the given eventid"""
        if not isinstance(val, LoadLimitEvent):
            msg = 'val expected LoadLimitEvent, got {} instead'
            raise TypeError(msg.format(type(val).__name__))
        self._event[eventid] = val

    def __getitem__(self, eventid):
        """Retrieve the event associated with the given eventid"""
        return self._event[eventid]

    def __contains__(self, eventid):
        """Determine if eventid exists"""
        return self._event.__contains__(eventid)

    def __bool__(self):
        """Return True if events have been stored"""
        return bool(self._event)

    def __call__(self, corofunc=None, eventid=None, *args, **kwargs):
        """Decorator to add a corofunc to an event

        If eventid is not specified, then the corofunc will be added to all
        events.

        """
        funclist = []

        def addpending(corofunc):
            """Add pending call"""
            self._pending_call.append(dict(corofunc=corofunc, args=args,
                                           kwargs=kwargs))

        def addcoro(corofunc):
            """Add corofunc"""
            addpending(corofunc)
            for func in funclist:
                func(corofunc)
            return corofunc

        if eventid is None:
            addfunc = funclist.append
            for event in self._event.values():
                ret = event.__call__(corofunc, *args, **kwargs)
                if corofunc is None:
                    addfunc(ret)
            if corofunc is None:
                corofunc = addcoro
            else:
                addpending(corofunc)
            return corofunc
        else:
            return self._event[eventid].__call__(corofunc, *args, **kwargs)

    def validate_factory(self, factory):
        """Return function wrapper that validates default factory return val"""

        def validate():
            """Make sure factory return value is an event"""
            event = factory()
            if not isinstance(event, LoadLimitEvent):
                msg = ('default_factory function returned {}, '
                       'expected LoadLimitEvent')
                raise TypeError(msg.format(type(event).__name__))

            # Process any pending calls
            for callargs in self._pending_call:
                event.__call__(callargs['corofunc'], *callargs['args'],
                               **callargs['kwargs'])
            return event

        return validate

    def keys(self):
        """Returns an iterator over event keys."""
        return self._event.keys()

    def values(self):
        """Returns an iterator over event values."""
        return self._event.values()

    def items(self):
        """Returns an iterator over event (key, value) pairs."""
        return self._event.items()

    def stop(self, eventid=None, ignore=None):
        """Stop the event

        This calls clear() first before stopping the event.

        """
        ignorefunc = self._ignore_exception
        if eventid is None:
            for event in self._event.values():
                ignorefunc(ignore, event.stop)
        else:
            ignorefunc(ignore, self._event[eventid].stop)

    def clear(self, eventid=None, ignore=None):
        """Reset the given event's internal flag to False.

        If eventid is None, will call clear() on all stored events.

        """
        ignorefunc = self._ignore_exception
        if eventid is None:
            for event in self._event.values():
                ignorefunc(ignore, event.clear)
        else:
            ignorefunc(ignore, self._event[eventid].clear)

    def is_set(self, eventid=None):
        """Return True iff internal flag for the given event is True

        If eventid is None, will return dict containing the results of
        each stored event's is_set().

        """
        if eventid is None:
            return {k: event.is_set() for k, event in self._event.items()}
        else:
            return self._event[eventid].is_set()

    def set(self, eventid=None, ignore=None, **kwargs):
        """Set internal flag for the given event to true.

        If eventid is None, will set every stored event.

        """
        kwargs['eventid'] = eventid
        ignorefunc = self._ignore_exception
        if eventid is None:
            for event in self._event.values():
                ignorefunc(ignore, event.set, **kwargs)
        else:
            ignorefunc(ignore, self._event[eventid].set, **kwargs)

    async def wait(self, eventid=None, ignore=None):
        """Block until the given event's internal flag is True

        If eventid is None, will block until every contained event's internal
        flag is True.

        """
        if eventid is None:
            loop = self._loop
            evdict = self._event
            ensure_future = asyncio.ensure_future
            tasks = [ensure_future(event.wait(), loop=loop)
                     for event in evdict.values()]
            await asyncio.gather(*tasks, loop=loop)
        else:
            await self._event[eventid].wait()

    def add(self, *tasks, eventid=None, ignore=None, **kwargs):
        """Add one or more tasks to the given event

        If eventid is None, adds the tasks to every stored event.

        """
        ignorefunc = self._ignore_exception
        if eventid is None:
            for event in self._event.values():
                ignorefunc(ignore, event.add, *tasks, **kwargs)
        else:
            ignorefunc(ignore, self._event[eventid].add, *tasks, **kwargs)

    def start(self, eventid=None, *, loop=None, ignore=None, **kwargs):
        """Start the given event.

        If eventid is None, starts all stored events.

        """
        self._loop = loop
        ignorefunc = self._ignore_exception
        if eventid is None:
            for event in self._event.values():
                ignorefunc(ignore, event.start, loop=loop, **kwargs)
        else:
            ignorefunc(ignore, self._event[eventid].start, loop=loop, **kwargs)

    def _ignore_exception(self, ignore, func, *args, **kwargs):
        """docstring for _ignore_exception"""
        if not ignore:
            ret = func(*args, **kwargs)
        else:
            try:
                ret = func(*args, **kwargs)
            except ignore as e:
                ret = None
        return ret


# ============================================================================
# Anchor
# ============================================================================


class Anchor(LoadLimitEvent):
    """Define a coro func that will run anchor all other tasks"""

    def __init__(self):
        super().__init__()
        self._anchortype = None
        self._anchorfunc = (None, None)

    def __call__(self, corofunc=None, anchortype=None, schedule=True,
                 **kwargs):
        """Decorator to add a corofunc to the event"""
        self.anchortype = anchortype
        if corofunc and anchortype is None and not schedule:
            self.add(corofunc, schedule=schedule, **kwargs)
            return corofunc
        return super().__call__(corofunc, schedule=schedule, **kwargs)

    def add(self, *tasks, schedule=True, **kwargs):
        """Adds tasks

        Prevents anchorfunc from being added as a normal task

        """
        if self._anchortype is None and not schedule:
            self._cb.extend(tasks)
        else:
            super().add(*tasks, **kwargs)

        # Set anchorfunc
        anchortype = self._anchortype
        if anchortype:
            self._anchorfunc = (anchortype, tasks[0])
            self._anchortype = None

    def _schedule_tasks(self, tasks, kwargs, loop):
        """Schedule all tasks

        lastfunc is not included as a normal task and is scheduled separately
        using the runlast coro.

        """
        anchortype, anchorfunc = self._anchorfunc
        if anchorfunc is None:
            super()._schedule_tasks(tasks, kwargs, loop)
        else:
            waiting = {t for t in tasks if t != anchorfunc}
            ensure_future(
                self.anchor(anchorfunc, anchortype, waiting, kwargs, loop),
                loop=loop)

    async def anchor(self, corofunc, anchortype, tasks, kwargs, loop):
        """Wait for the event and all other tasks before running corofunc"""
        while True:
            future = self._result
            await self.wait()
            result = Namespace(**future.result())

            # Run the first coro
            if anchortype == AnchorType.first:
                await corofunc(result, **kwargs)

            # Run async callbacks
            async for cb in ageniter(self._cb):
                await cb(result, **kwargs)

            # schedule all tasks
            if tasks:
                self._waiting = waiting = {
                    ensure_future(coro(result, **kwargs), loop=loop)
                    for coro in tasks
                }

            # Wait for all other tasks to finish
            waiting = self._waiting
            if waiting:
                await asyncio.gather(*waiting, loop=loop)

            waiting.clear()

            # Run the last coro
            if anchortype == AnchorType.last:
                await corofunc(result, **kwargs)

            if not self._option.reschedule:
                break

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


# ============================================================================
# RunLast
# ============================================================================


class RunLast(Anchor):
    """Define a coro func that will run after all other tasks"""

    def __call__(self, corofunc=None, runlast=False, schedule=True):
        """Decorator to add a corofunc to the event"""
        anchortype = AnchorType.last if runlast else None
        return super().__call__(corofunc, anchortype=anchortype,
                                schedule=schedule)


# ============================================================================
# RunFirst
# ============================================================================


class RunFirst(Anchor):
    """Define a coro func that will run before all other tasks"""

    def __call__(self, corofunc=None, runfirst=False, schedule=True):
        """Decorator to add a corofunc to the event"""
        anchortype = AnchorType.first if runfirst else None
        return super().__call__(corofunc, anchortype=anchortype,
                                schedule=schedule)


# ============================================================================
# Events
# ============================================================================


shutdown = RunLast()


# ============================================================================
#
# ============================================================================
