# -*- coding: utf-8 -*-
# loadlimit/cli.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Define CLI"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from argparse import ArgumentParser
import asyncio
from collections import defaultdict
from contextlib import contextmanager, ExitStack
from functools import partial
from importlib import import_module
from itertools import count
import sys

# Third-party imports
from pandas import Timedelta
from pytz import timezone
from tqdm import tqdm

# Local imports
from . import event
from .core import BaseLoop, Client
from .importhook import TaskImporter
from .util import aiter, Namespace


# ============================================================================
# Globals
# ============================================================================


PROGNAME = 'loadlimit'


# ============================================================================
# Helpers
# ============================================================================


def commalist(commastr):
    """Transforms a comma-delimited string into a list of strings"""
    return [] if not commastr else [c for c in commastr.split(',') if c]


# ============================================================================
# tqdm integration
# ============================================================================


async def update_tqdm(config, state, name):
    """Update tqdm display"""
    counter = count()
    cur = next(counter)
    while True:
        cur = next(counter)
        await asyncio.sleep(1)
        pbar = state.progressbar[name]
        if pbar.total is None or cur < pbar.total:
            pbar.update(1)
            state.tqdm_progress[name] = cur

        if state.reschedule is False:
            return

async def stop_tqdm(result,*, manager=None, state=None, name=None):
    """Stop tqdm updating"""
    progress = state.tqdm_progress[name]
    pbar = state.progressbar[name]
    if pbar.total is not None and progress < pbar.total:
        pbar.update(pbar.total - progress)
        state.tqdm_progress[name] = pbar.total


@contextmanager
def tqdm_context(config, state, *, name=None, sched=False, **kwargs):
    """Setup tqdm"""
    # Do nothing
    if not config['loadlimit']['show-progressbar']:
        yield
        return

    # Setup tqdm
    with tqdm(**kwargs) as pbar:
        if name is not None:
            state.progressbar[name] = pbar
            state.tqdm_progress[name] = 0
            if sched:
                asyncio.ensure_future(update_tqdm(config, state, name))
                event.shutdown(partial(stop_tqdm, state=state, name=name),
                               schedule=False)
        try:
            yield pbar
        finally:
            if name is not None:
                state.progressbar[name] = None


class TQDMClient(Client):
    """tqdm-aware client"""

    async def __call__(self, state):
        pbarkey = 'iteration'
        pbar = state.progressbar.get(pbarkey, None)
        if pbar is None:
            await super().__call__(state)
            return

        ensure_future = asyncio.ensure_future
        while True:
            t = [ensure_future(corofunc(state)) for corofunc in self._corofunc]
            await asyncio.gather(*t)
            pbar.update(1)
            state.tqdm_progress[pbarkey] += 1

            if not self.option.reschedule:
                return

    async def init(self, config, state):
        """Initialize the client"""
        pbarkey = 'init'
        await super().init(config, state)
        pbar = state.progressbar.get(pbarkey, None)
        if pbar is not None:
            pbar.update(1)
            state.tqdm_progress[pbarkey] += 1


# ============================================================================
# MainLoop
# ============================================================================


class MainLoop(BaseLoop):
    """Integrates with Client"""

    def __init__(self, *args, clientcls=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._clients = frozenset()
        self._clientcls = Client if clientcls is None else clientcls

    def init(self, config, state):
        """Initialize clients"""
        clients = frozenset(self.spawn_clients(config))
        self._clients = clients
        loop = self.loop

        # Init clients
        ensure_future = asyncio.ensure_future
        t = [ensure_future(c.init(config, state), loop=loop)
             for c in clients]
        f = asyncio.gather(*t, loop=loop)
        loop.run_until_complete(f)

        # Schedule loop end
        ensure_future(self.endloop(config, state), loop=loop)

        # Schedule clients on the loop
        for c in clients:
            ensure_future(c(state), loop=loop)

    def spawn_clients(self, config):
        """Spawns clients according the given config"""
        taskmod = import_module('loadlimit.task')
        tasklist = taskmod.__tasks__
        numclients = config['loadlimit']['numusers']
        clientcls = self._clientcls
        tasks = [clientcls(tasklist, reschedule=True)
                 for _ in range(numclients)]
        return tasks

    async def endloop(self, config, state):
        """coro func that ends the loop after a given duration"""
        duration = config['loadlimit']['duration']
        await asyncio.sleep(duration.total_seconds())

        # Stop rescheduling clients
        state.reschedule = False
        async for client in aiter(self.clients):
            client.option.reschedule = False

        # Call shutdown event
        event.shutdown.set(exitcode=0)

    @property
    def clients(self):
        """Return spawned clients"""
        return self._clients


# ============================================================================
#
# ============================================================================


def process_options(config, args):
    """Process options defined in defaultoptions()"""
    llconfig = config['loadlimit']

    # Set timezone info
    llconfig['timezone'] = timezone(args.timezone)

    # Setup number of users
    numusers = args.numusers
    if numusers == 0:
        raise ValueError('users option expected value > 0, got {}'.
                         format(numusers))
    llconfig['numusers'] = numusers

    # Set duration
    delta = Timedelta(args.duration)
    if not isinstance(delta, Timedelta):
        raise ValueError('duration option got invalid value {!r}'.
                         format(args.duration))
    llconfig['duration'] = delta

    # Setup TaskImporter
    llconfig['importer'] = TaskImporter(*args.taskfile)

    llconfig['show-progressbar'] = args.progressbar


def defaultoptions(parser):
    """Create locust command"""
    parser.add_argument('-c', '--config', default=None,
                        metavar='CONFIG',
                        help='Load configuration file %(metavar)s')

    parser.add_argument(
        '-u', '--users', dest='numusers', default=1, type=int,
        help='Number of users/clients to simulate'
    )

    parser.add_argument(
        '-d', '--duration', dest='duration', default=None,
        help='The length of time the load test will run for'
    )

    parser.add_argument(
        '-t', '--task', dest='taskname', metavar='TASKNAME',
        default=None, nargs='+',
        help=('Task names to schedule')
    )

    parser.add_argument(
        '--timezone', dest='timezone', default='UTC',
        help='Timezone to display dates in (default: UTC)'
    )

    parser.add_argument(
        '--no-progressbar', dest='progressbar', action='store_false',
        help='Timezone to display dates in (default: UTC)'
    )

    parser.add_argument(
        'taskfile', metavar='FILE', nargs='+',
        help='Python module file to import as a task file'
    )

    parser.set_defaults(_main=runloop)


def create():
    """Construct basic cli interface"""
    parser = ArgumentParser(prog=PROGNAME)

    # Create loadlimit command
    defaultoptions(parser)

    return parser


def runloop(config, args, state):
    """Process cli options and start the loop"""
    llconfig = config['loadlimit']

    # Process cli options
    process_options(config, args)

    # Set up importhook
    sys.meta_path.append(llconfig['importer'])

    # Create state namespace
    state.reschedule = True
    state.progressbar = {}
    state.tqdm_progress = {}

    # Run the loop
    with ExitStack() as stack:
        main = stack.enter_context(MainLoop(clientcls=TQDMClient))

        # Create and initialize clients
        numusers = llconfig['numusers']
        with tqdm_context(config, state, name='init', desc='Ramp-up',
                          total=numusers):
            main.init(config, state)

        # Re-enter tqdm context
        duration = int(llconfig['duration'].total_seconds())
        stack.enter_context(tqdm_context(config, state, name='runtime',
                                         total=duration, desc='Run time',
                                         sched=True))
        stack.enter_context(tqdm_context(config, state, name='iteration',
                                         desc='Iterations'))

        # Start the loop
        main.start()

    return main.exitcode


# ============================================================================
# Main
# ============================================================================


def main(arglist=None, config=None, state=None):
    """Main cli interface"""
    if not arglist:
        arglist = sys.argv[1:]
        if not arglist:
            arglist.append('--help')
    if config is None:
        config = defaultdict(dict)

    # Ensure loadlimit config section exists
    if 'loadlimit' not in config:
        config['loadlimit'] = {}

    if state is None:
        state = Namespace()

    parser = create()
    args = parser.parse_args(arglist)
    exitcode = args._main(config, args, state)
    sys.exit(exitcode)


if __name__ == '__main__':
    main()


# ============================================================================
#
# ============================================================================
