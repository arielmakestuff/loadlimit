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
from functools import partial
import logging
from signal import SIGTERM, SIGINT, signal as setupsignal
import sys

# Third-party imports

# Local imports
from .util import LogLevel
from . import event


# ============================================================================
# Coroutines
# ============================================================================


@event.shutdown(runlast=True)
async def shutdown(result, *, manager=None):
    """Coroutine that shuts down the loop"""
    # manager is an instance of BaseLoop
    manager.logger.info('shutdown')
    manager._loopend.set_result(result.exitcode)


# ============================================================================
# BaseLoop
# ============================================================================


class BaseLoop:
    """Setup event loop - base implementation"""

    def __init__(self, *, loop=None, logname='loadlimit',
                 loglevel=LogLevel.INFO):
        # Setup logging
        self._loglevel = None
        self._logname = logname
        self.loglevel = loglevel

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
        event.shutdown.start(loop=loop, manager=self)

        return self

    def __exit__(self, exctype, exc, tb):
        """Perform cleanup tasks"""
        self.cleanup()
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
        tasks = asyncio.Task.all_tasks()
        if all(t.done() for t in tasks):
            return
        logger = self.logger
        logger.info('cancelling tasks')
        future = asyncio.gather(*tasks)
        future.cancel()
        try:
            self._loop.run_until_complete(future)
        except CancelledError:
            pass
        logger.info('tasks cancelled')

    def initlogging(self):
        """Setup logging"""
        level = self._loglevel.value
        logger = logging.getLogger(self._logname)
        logger.setLevel(level)

        # Set level for asyncio logger
        logging.getLogger('asyncio').setLevel(level)

        # Clear out any existing handlers
        if logger.handlers:
            allh = logger.handlers.copy()
            for h in allh:
                logger.removeHandler(h)

        # Create formatter
        msgformat = '%(message)s'
        formatter = logging.Formatter(msgformat)

        # Create console handler
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(formatter)

        # Put it all together
        logger.addHandler(ch)

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
        future = context['future']
        self.logger.info('got exception: {}'.format(future.exception()))
        event.shutdown.set(exitcode=1)

    def stopsignal(self, sig, exitcode, *args):
        """Schedule shutdown"""
        self.logger.info('got signal {} ({})'.format(sig.name, sig))
        event.shutdown.set(exitcode=exitcode)

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
        return logging.getLogger(self._logname)

    @property
    def loglevel(self):
        """Return the loglevel"""
        return self._loglevel

    @loglevel.setter
    def loglevel(self, newlevel):
        """Set a new loglevel"""
        if not isinstance(newlevel, LogLevel):
            msg = 'loglevel expected LogLevel, got {}'
            raise TypeError(msg.format(newlevel.__class__.__name__))
        self._loglevel = newlevel
        self.initlogging()


# ============================================================================
# TaskABC
# ============================================================================


class TaskABC(metaclass=ABCMeta):
    """ABC definition of a loadlimit task"""

    @abstractmethod
    async def __call__(self):
        raise NotImplementedError

    @abstractmethod
    async def init(self, config):
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
        self._corofunc = corofunc()

    async def __call__(self):
        await self._corofunc

    async def init(self, config):
        """Initialize the task"""
        pass


# ============================================================================
#
# ============================================================================
