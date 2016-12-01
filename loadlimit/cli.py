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
import os
from os.path import abspath, isdir, join as pathjoin
import sys
from tempfile import TemporaryDirectory

# Third-party imports
from pandas import Timedelta
from pytz import timezone
from sqlalchemy import create_engine
from tqdm import tqdm

# Local imports
from . import channel
from . import stat
from .core import BaseLoop, Client
from .event import NoEventTasksError
from .importhook import TaskImporter
from .stat import (flushtosql, flushtosql_shutdown, Period, SQLTimeSeries,
                   SQLTotal, TimeSeries, Total)
from .util import aiter, LogLevel, Namespace


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

async def stop_tqdm(exitcode, *, manager=None, state=None, name=None):
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
                channel.shutdown(partial(stop_tqdm, state=state, name=name))
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

        # Send shutdown command
        await channel.shutdown.send(0)

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

    # tqdm
    llconfig['show-progressbar'] = args.progressbar

    # Setup cache
    cache_type = args.cache
    llconfig['cache'] = dict(type=cache_type)

    # Setup export
    llconfig['export'] = exportsection = {}
    exportsection['type'] = export = args.export
    if export is not None:
        exportdir = args.exportdir
        if exportdir is None:
            exportdir = os.getcwd()
        if not isdir(exportdir):
            raise FileNotFoundError(exportdir)
        exportsection['targetdir'] = exportdir

    # Setup periods
    if args.periods <= 1:
        raise ValueError('periods option must be > 1')
    llconfig['periods'] = args.periods


def defaultoptions(parser):
    """cli arguments"""
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

    # cache arguments
    parser.add_argument(
        '-C', '--cache', dest='cache', choices=['memory', 'sqlite'],
        default='memory',
        help='What type of storage to use as the cache. Default: memory'
    )

    # export arguments
    parser.add_argument(
        '-E', '--export', dest='export', choices=['csv', 'sqlite'],
        default=None,
        help='What type of file to export results to.'
    )

    parser.add_argument(
        '-e', '--export-dir', dest='exportdir', default=None,
        help='The directory to export results to.'
    )

    parser.add_argument(
        '-p', '--periods', dest='periods', type=int, default=8,
        help='The number of time periods to show in the results. Default: 8'
    )

    # taskfiles
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


class StatSetup:
    """Context setting up time recording and storage"""

    def __init__(self, config, state):
        self._config = config
        self._state = state
        self._calcobj = (None, None)
        self._results = None
        self._statsdict = None

    def __enter__(self):
        config = self._config
        state = self._state
        llconfig = config['loadlimit']
        self._statsdict = statsdict = Period()

        if llconfig['cache']['type'] == 'memory':
            self._calcobj = tuple(c(statsdict=statsdict)
                                  for c in [Total, TimeSeries])
            state.sqlengine = None
        else:
            cachefile = pathjoin(llconfig['tempdir'], 'cache.db')
            connstr = 'sqlite:///{}'.format(cachefile)
            state.sqlengine = engine = create_engine(connstr)
            self._calcobj = tuple(
                c(statsdict=statsdict, sqlengine=engine)
                for c in [SQLTotal, SQLTimeSeries])

            # Subscribe to shutdown command
            channel.shutdown(partial(flushtosql_shutdown, statsdict=statsdict,
                                     sqlengine=engine))

            # Add flushtosql to recordperiod event
            stat.recordperiod(flushtosql, schedule=False)

        return self

    def __exit__(self, errtype, err, errtb):
        total, timeseries = self._calcobj
        statsdict = self._statsdict
        with ExitStack() as stack:
            # Set timeseries periods
            timeseries.vals.periods = self._config['loadlimit']['periods']

            if statsdict.start_date is None:
                return

            # Enter results contexts
            for r in [total, timeseries]:
                stack.enter_context(r)

            # Run calculations
            for name, df in total:
                total.calculate(name, df)
                timeseries.calculate(name, df)

        self._results = (total.vals.results, ) + timeseries.vals.results

        # Don't export
        exportconfig = self._config['loadlimit']['export']
        export_type = exportconfig['type']
        if export_type is None:
            return

        exportdir = exportconfig['targetdir']

        # Export values
        for calcobj in [total, timeseries]:
            calcobj.export(export_type, exportdir)

    def startevent(self):
        """Start events"""
        engine = self._state.sqlengine
        stat.recordperiod.start(ignore=NoEventTasksError, reschedule=True,
                                statsdict=self._statsdict, sqlengine=engine)

    @property
    def results(self):
        """Return stored results"""
        return self._results


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

    statsetup = StatSetup(config, state)

    # Run the loop
    with ExitStack() as stack:
        # Setup a temporary directory
        llconfig['tempdir'] = abspath(
            stack.enter_context(TemporaryDirectory()))

        # Setup stats recording
        stack.enter_context(statsetup)

        # Enter main loop
        main = stack.enter_context(MainLoop(loglevel=LogLevel.WARNING,
                                            clientcls=TQDMClient))

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

        # Start events
        statsetup.startevent()

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
