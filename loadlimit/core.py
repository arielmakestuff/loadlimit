# -*- coding: utf-8 -*-
# loadlimit/core.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Core loadlimit functionality"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from abc import ABCMeta, abstractmethod
import asyncio
from asyncio import (CancelledError, InvalidStateError, iscoroutinefunction,
                     sleep)
from collections.abc import Iterable
from functools import partial
from itertools import chain
import logging
from signal import SIGTERM, SIGINT, signal as setupsignal
import sys

# Third-party imports

# Local imports
from . import channel
from .channel import AnchorType
from .util import Logger, LogLevel, Namespace


# ============================================================================
# Coroutines
# ============================================================================


@channel.shutdown(anchortype=AnchorType.last)
async def shutdown(exitcode, *, manager=None):
    """Coroutine that shuts down the loop"""
    # manager is an instance of BaseLoop
    manager.logger.info('shutdown')
    manager._loopend.set_result(exitcode)


# ============================================================================
# BaseLoop
# ============================================================================


class BaseLoop:
    """Setup event loop - base implementation"""

    def __init__(self, *, loop=None, logname='loadlimit',
                 loglevel=LogLevel.INFO):
        # Setup logging
        self._logger = Logger(name=logname)
        self._logoptions = o = {'init-logging': False,
                                'name': logname}
        self.loglevel = loglevel
        o['init-logging'] = True

        # Setup loop
        if (loop is not None and
                not isinstance(loop, asyncio.AbstractEventLoop)):
            msg = 'loop expected AbstractEventLoop, got {} instead'
            raise TypeError(msg.format(type(loop).__name__))

        self._loop = asyncio.new_event_loop() if loop is None else loop
        self._loopend = None

    # --------------------
    # Context
    # --------------------

    def __enter__(self):
        """Setup main loop"""
        # Setup main loop
        loop = self._loop
        asyncio.set_event_loop(loop)
        self._loopend = self.create_endfuture(loop)

        # Setup error handler
        loop.set_exception_handler(self.uncaught_exceptions)

        # Setup signal handler
        self.initsignals()

        # Start shutdown event
        channel.shutdown.open()
        channel.shutdown.start(loop=loop, manager=self)

        return self

    def __exit__(self, exctype, exc, tb):
        """Perform cleanup tasks"""
        channel.shutdown.stop()
        self.cleanup()
        channel.shutdown.close()
        self._loop.close()
        self._loop = None
        self.logger.info('loop closed')

    # --------------------
    # Helpers
    # --------------------

    def create_endfuture(self, loop):
        """Create future representing the end of the main loop"""
        loopend = loop.create_future()
        loopend.add_done_callback(self.stoploop)
        return loopend

    def initsignals(self):
        """Setup SIGTERM and SIGINT handling"""
        platform = sys.platform
        for sig in [SIGTERM, SIGINT]:
            if platform == 'win32':
                setupsignal(sig, partial(self.stopsignal, sig, 0))
            else:
                self._loop.add_signal_handler(sig, self.stopsignal, sig, 0)

        if platform == 'win32':
            asyncio.ensure_future(self.always_sleep())

    def cleanup(self):
        """Cancel any remaining tasks in the loop"""
        # Wait for shutdown channel to complete
        self._loop.run_until_complete(channel.shutdown.join())

        tasks = asyncio.Task.all_tasks()
        # if all(t.done() for t in tasks):
        #     return
        logger = self.logger
        logger.info('cancelling tasks')
        future = asyncio.gather(*tasks)
        self._loop.call_soon_threadsafe(future.cancel)
        try:
            self._loop.run_until_complete(future)
        except CancelledError:
            pass
        logger.info('tasks cancelled')

    def initloghandlers(self, formatter):
        """Setup log handlers"""
        level = self.loglevel.value

        # Create console handler
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(formatter)
        return [ch]

    def initlogformatter(self):
        """Setup log formatter"""
        options = self._logoptions
        fmtcls = options.get('fmtcls', logging.Formatter)
        style = options.get('style', '%')
        msgformat = options.get('format', None)
        datefmt = options.get('datefmt', None)
        formatter = fmtcls(msgformat, datefmt=datefmt, style=style)
        return formatter

    def initlogging(self, **kwargs):
        """Setup logging"""
        options = self._logoptions
        options.update(kwargs)
        level = options['level'].value
        logger = self._logger.logger
        logger.setLevel(level)

        # Set level for asyncio logger
        logging.getLogger('asyncio').setLevel(level)

        # Clear out any existing handlers
        if logger.handlers:
            allh = logger.handlers.copy()
            for h in allh:
                logger.removeHandler(h)

        # Create formatter
        formatter = self.initlogformatter()

        # Put it all together
        for h in self.initloghandlers(formatter):
            logger.addHandler(h)

    def start(self):
        """Start the loop"""
        self.logger.info('loop started')
        self._loop.run_forever()

    def run(self, tasksiter=()):
        """Setup and run the loop"""
        ensure_future = asyncio.ensure_future
        with self as loop:
            for t in tasksiter:
                ensure_future(t)
            loop.start()
        return self.exitcode

    # --------------------
    # Handlers
    # --------------------

    def stoploop(self, loopend):
        """Callback to stop the main loop"""
        self.logger.info('stopping loop')
        self._loop.stop()

    def uncaught_exceptions(self, loop, context):
        """Handle uncaught exceptions"""
        exception = (context['future'].exception() if 'future' in context
                     else context['exception'])
        self.logger.error('got exception: {}', exception, exc_info=True)
        channel.shutdown.put(1)

    def stopsignal(self, sig, exitcode, *args):
        """Schedule shutdown"""
        self.logger.info('got signal {} ({})'.format(sig.name, sig))
        channel.shutdown.put(exitcode)

    async def always_sleep(self, duration=1):
        """Coroutine that always sleep for a given duration"""
        while True:
            await sleep(duration)

    # --------------------
    # Info
    # --------------------

    def running(self):
        """Returns True if the main loop is still running"""
        return False if self._loop is None else self._loop.is_running()

    @property
    def exitcode(self):
        """Return the exit code"""
        loopend = self._loopend
        if loopend is None:
            return None
        try:
            exitcode = self._loopend.result()
        except InvalidStateError:
            exitcode = None
        return exitcode

    @property
    def logger(self):
        """Return the logger set by this instance"""
        return self._logger

    @property
    def logoptions(self):
        """Retrieve log options"""
        return self._logoptions

    @property
    def loglevel(self):
        """Return the loglevel"""
        return self._logoptions['level']

    @loglevel.setter
    def loglevel(self, newlevel):
        """Set a new loglevel"""
        if not isinstance(newlevel, LogLevel):
            msg = 'loglevel expected LogLevel, got {}'
            raise TypeError(msg.format(newlevel.__class__.__name__))
        logoptions = self._logoptions
        logoptions['level'] = newlevel
        if logoptions.get('init-logging', True):
            self.initlogging()

    @property
    def loop(self):
        """Return the asyncio loop"""
        return self._loop


# ============================================================================
# TaskABC
# ============================================================================


class TaskABC(metaclass=ABCMeta):
    """ABC definition of a loadlimit task"""

    @abstractmethod
    async def __call__(self, state):
        raise NotImplementedError

    @abstractmethod
    async def init(self, config, state):
        """Coroutine that initializes the task """
        raise NotImplementedError


# ============================================================================
# Task
# ============================================================================


class Task(TaskABC):
    """Wraps coroutine in a task"""

    def __init__(self, corofunc):
        if not iscoroutinefunction(corofunc):
            msg = 'corofunc expected coroutine function, got {} instead'
            raise TypeError(msg.format(type(corofunc).__name__))
        self._corofunc = corofunc

    async def __call__(self, state):
        await self._corofunc()

    async def init(self, config, state):
        """Initialize the task"""
        pass


# ============================================================================
# Client
# ============================================================================


class Client(TaskABC):
    """Determine how to run a set of tasks"""
    mindelay = 0
    maxdelay = 0

    def __init__(self, *cf_or_cfiter, reschedule=False):
        self._option = Namespace(reschedule=reschedule)
        cfiter = []
        cflist = []
        for cf in cf_or_cfiter:
            cfstore = cfiter if isinstance(cf, Iterable) else cflist
            cfstore.append(cf)

        corofunc = []
        for cf in chain(cflist, *cfiter):
            isclass = isinstance(cf, type)
            if (isinstance(cf, TaskABC) or
                    (isclass and cf is not TaskABC and
                     issubclass(cf, TaskABC))):
                if isclass:
                    cf = cf()
                corofunc.append(cf)
                continue

            msg = 'cf_or_cfiter expected TaskABC subclass, got {} instead'
            raise TypeError(msg.format(type(cf).__name__))

        if not corofunc:
            msg = 'Client object did not receive any TaskABC subclasses'
            raise ValueError(msg)
        self._corofunc = corofunc

    async def __call__(self, state):
        ensure_future = asyncio.ensure_future
        while True:
            t = [ensure_future(corofunc(state)) for corofunc in self._corofunc]
            await asyncio.gather(*t)

            if not self.option.reschedule:
                return

    async def init(self, config, state):
        """Initialize the task"""
        ensure_future = asyncio.ensure_future
        t = [ensure_future(corofunc.init(config, state))
             for corofunc in self._corofunc]
        await asyncio.gather(*t)

    @property
    def option(self):
        """Return the option namespace"""
        return self._option


# ============================================================================
#
# ============================================================================
