# -*- coding: utf-8 -*-
# loadlimit/generator/data.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

""""""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from collections import namedtuple
from enum import Enum
from functools import partial

# Third-party imports

# Local imports
from ..core import BaseLoop, Client


# ============================================================================
# Messages
# ============================================================================


MessageType = Enum('Message', ['ok', 'data', 'shutdown'])


GenericMessage = namedtuple('Generic', 'type')


DataMessage = namedtuple('Data', 'type data')


class Message(Enum):
    ok = partial(GenericMessage, type=MessageType.ok)
    data = partial(DataMessage, type=MessageType.data)
    shutdown = partial(GenericMessage, type=MessageType.shutdown)


def mkmsg(msg, *, data=None):
    """Create a message"""
    if not isinstance(msg, Message):
        errmsg = ('msg arg expected {} object, got {} object instead'.
                  format(Message.__name__, type(msg).__name__))
        raise TypeError(errmsg)

    return msg.value(data=data) if msg == Message.data else msg.value()


# ============================================================================
# Improved TaskABC
# ============================================================================


class TaskABC(metaclass=ABCMeta):
    """ABC definition of a loadlimit task"""
    __slots__ = ()

    @abstractmethod
    async def __call__(self, state, *, clientid=None):
        raise NotImplementedError

    @abstractmethod
    def __copy__(self):
        """Create a copy of current task"""
        raise NotImplementedError

    @abstractmethod
    async def init(self, config, state):
        """Coroutine that initializes the task """
        raise NotImplementedError

    @abstractmethod
    async def shutdown(self, config, state):
        """Coroutine that shuts down the task"""
        raise NotImplementedError

    @abstractmethod
    @classmethod
    async def create(cls, config, state):
        """"""
        return cls()


# ============================================================================
# DataLoop
# ============================================================================


class TimeSeriesDataLoop(BaseLoop):
    """Event loop that generates time series data"""

    def __init__(self, *args, clientcls=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = None
        self._clientcls = Client if clientcls is None else clientcls

    def init(self, config, state):
        """Initialize clients"""
        countstore = state.countstore
        self._client = client = self.spawn_client(config)
        loop = self.loop
        ensure_future = asyncio.ensure_future

        # Init client
        loop.run_until_complete(ensure_future(client.init(config, state), loop=loop))

        # Clear countstore
        countstore.reset()

        # Schedule loop end
        ensure_future(self.endloop(config, state), loop=loop)

        # Schedule clients on the loop
        ensure_future(client(state), loop=loop)

    def shutdown(self, config, state):
        """Shutdown clients"""
        loop = self.loop

        # Tell clients to shutdown
        ensure_future = asyncio.ensure_future
        t = [ensure_future(c.shutdown(config, state), loop=loop)
             for c in self._clients]
        f = asyncio.gather(*t, loop=loop)
        loop.run_until_complete(f)

    def spawn_client(self, config):
        """Spawns clients according the given config"""
        taskmod = import_module('loadlimit.task')
        tasklist = taskmod.__tasks__
        client = self._clientcls(tasklist, reschedule=False)
        return client

    async def endloop(self, config, state):
        """coro func that ends the loop after a given duration"""
        duration = config['loadlimit']['duration']
        await asyncio.sleep(duration.total_seconds())

        # Stop rescheduling clients
        state.reschedule = False
        async for client in aiter(self.clients):
            client.option.reschedule = False

        # Send shutdown command
        await channel.shutdown.send(0)


# ============================================================================
# Main
# ============================================================================


def main(msgqueue, config, data):
    """Main function that handles messages from supervisor"""



# ============================================================================
#
# ============================================================================
