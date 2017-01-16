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
from datetime import datetime
from functools import partial
from importlib import import_module
from itertools import count
from logging import FileHandler, Formatter
import os
from os.path import abspath, isdir, join as pathjoin
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import time

# Third-party imports
from pandas import Timedelta
from pytz import timezone
from sqlalchemy import create_engine
from tqdm import tqdm

# Local imports
from . import channel
from . import stat
from .core import BaseLoop, Client
from .importhook import TaskImporter
from .result import (SQLTimeSeries, SQLTotal, SQLTotalError, SQLTotalFailure,
                     TimeSeries, Total, TotalError, TotalFailure)
from .stat import (flushtosql, flushtosql_shutdown, measure, Period,
                   SendTimeData)
from .util import aiter, Event, EventType, LogLevel, Namespace, TZ_UTC


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


class LoadLimitFormatter(Formatter):
    """Define nanoseconds for formatTime"""

    converter = partial(datetime.fromtimestamp, tz=TZ_UTC)

    def formatTime(self, record, datefmt=None):
        """
        Return the creation time of the specified LogRecord as formatted text.

        This method should be called from format() by a formatter which wants
        to make use of a formatted time. This method can be overridden in
        formatters to provide for any specific requirement, but the basic
        behaviour is as follows: if datefmt (a string) is specified, it is used
        with datetime.strftime() to format the creation time of the record.
        Otherwise, the ISO8601 format is used. The resulting string is
        returned. This function uses a user-configurable function to convert
        the creation time to a tuple. By default,
        datetime.datetime.fromtimestamp() is used; to change this for a
        particular formatter instance, set the 'converter' attribute to a
        function with the same signature as time.localtime() or time.gmtime().
        To change it for all formatters, for example if you want all logging
        times to be shown in GMT, set the 'converter' attribute in the
        Formatter class.
        """
        ct = self.converter(record.created)
        if datefmt:
            ct = self.converter(record.created)
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime(self.default_time_format)
            s = self.default_msec_format % (t, record.msecs)
        return s


class Printer:
    """Helper class for printing to stdout"""

    def __init__(self, printfunc=None):
        self._printfunc = None
        self.printfunc = print if printfunc is None else printfunc

    def __call__(self, *value, sep=' ', end='\n', file=sys.stdout,
                 flush=False, startnewline=False):
        """Print values"""
        printfunc = self._printfunc
        if flush:
            file.flush()
        if startnewline:
            self.__call__('\n', flush=True)
        if printfunc is print:
            printfunc(*value, sep=sep, end=end, file=file)
        else:
            msg = '{}{}'.format(sep.join(str(v) for v in value), end)
            printfunc(msg)

    @property
    def printfunc(self):
        """Return current printfunc"""
        return self._printfunc

    @printfunc.setter
    def printfunc(self, func):
        """Set new printfunc"""
        if not callable(func):
            msg = ('printfunc expected callable, got value of type {}'.
                   format(type(func).__name__))
            raise TypeError(msg)
        self._printfunc = func


cleanup = channel.DataChannel(name='cleanup')


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


class TQDMCleanup:
    """Update cleanup progress bar"""

    def __init__(self, config, state):
        self._prev = None
        self._state = state

    async def __call__(self, qsize):
        state = self._state
        pbarkey = 'cleanup'
        pbar = state.progressbar.get(pbarkey, None)
        if pbar is None:
            return
        prev = self._prev
        if prev is None:
            self._prev = qsize
            pbar.total = qsize
            return
        update = prev - qsize
        self._prev = qsize
        pbar.update(update)
        state.tqdm_progress[pbarkey] += update


@contextmanager
def tqdm_context(config, state, *, name=None, sched=False, **kwargs):
    """Setup tqdm"""
    # Do nothing
    if not config['loadlimit']['show-progressbar']:
        yield
        return

    # Setup tqdm
    with tqdm(**kwargs) as pbar:
        oldprinter = state.write.printfunc
        if name is not None:
            state.progressbar[name] = pbar
            state.tqdm_progress[name] = 0
            state.write.printfunc = tqdm.write
            if sched:
                asyncio.ensure_future(update_tqdm(config, state, name))
                channel.shutdown(partial(stop_tqdm, state=state, name=name))
        try:
            yield pbar
        finally:
            if name is not None:
                state.write.printfunc = oldprinter
                state.progressbar[name] = None


class TQDMClient(Client):
    """tqdm-aware client"""
    __slots__ = ()

    async def __call__(self, state, *, clientid=None):
        if clientid is None:
            clientid = self.id
        pbarkey = 'iteration'
        pbar = state.progressbar.get(pbarkey, None)
        if pbar is None:
            await super().__call__(state, clientid=clientid)
            return

        ensure_future = asyncio.ensure_future
        while True:
            t = [ensure_future(corofunc(state, clientid=clientid))
                 for corofunc in self._corofunc]
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

    async def shutdown(self, config, state):
        """Shutdown the client"""
        pbarkey = 'shutdown'
        await super().shutdown(config, state)
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

    def initloghandlers(self, formatter):
        """Setup log handlers"""
        ret = super().initloghandlers(formatter)
        options = self._logoptions
        logfile = options.get('logfile', None)
        if logfile:
            fh = FileHandler(logfile)
            fh.setLevel(LogLevel.DEBUG.value)
            fh.setFormatter(formatter)
            ret = [fh]
        return ret

    def initlogformatter(self):
        """Setup log formatter"""
        options = self._logoptions
        formatter = super().initlogformatter()
        formatter.converter = partial(datetime.fromtimestamp, tz=options['tz'])
        return formatter

    async def initclients(self, config, state, clients, loop):
        """Initialize clients according to the given rate"""
        if not clients:
            return
        clients = set(clients)
        rate = config['loadlimit']['initrate']
        ensure_future = asyncio.ensure_future
        numclients = len(clients)
        if rate == 0:
            rate = numclients
        tasks = []
        addtask = tasks.append
        state.event.append(Event(EventType.init_start))
        while True:
            if numclients <= rate:
                async for c in aiter(clients):
                    addtask(ensure_future(c.init(config, state), loop=loop))
                break
            async for i in aiter(range(rate)):
                c = clients.pop()
                addtask(ensure_future(c.init(config, state), loop=loop))
            numclients = len(clients)
            await asyncio.sleep(1)
        await asyncio.gather(*tasks, loop=loop)
        state.event.append(Event(EventType.init_end))

    async def schedclients(self, config, state, clients, loop):
        """Schedule clients according to schedsize and sched_delay"""
        if not clients:
            return
        size = config['loadlimit']['schedsize']
        delay = config['loadlimit']['sched_delay'].total_seconds()
        ensure_future = asyncio.ensure_future
        sleep = asyncio.sleep
        clients = set(clients)
        numclients = len(clients)
        numsched = 0
        if size == 0:
            size = numclients
        if size == numclients:
            delay = 0
        state.event.append(Event(EventType.warmup_start))
        while numsched < numclients:
            if not state.reschedule:
                break
            for i in range(size):
                c = clients.pop()
                ensure_future(c(state), loop=loop)
            numsched = numsched + size
            await sleep(delay)
        state.event.append(Event(EventType.warmup_end))

    def init(self, config, state):
        """Initialize clients"""
        countstore = state.countstore
        clients = frozenset(self.spawn_clients(config))
        self._clients = clients
        loop = self.loop
        ensure_future = asyncio.ensure_future

        # Init clients
        loop.run_until_complete(self.initclients(config, state, clients, loop))

        # Clear countstore
        countstore.reset()

        # Schedule loop end
        ensure_future(self.endloop(config, state), loop=loop)

        # Schedule clients on the loop
        ensure_future(self.schedclients(config, state, clients, loop),
                      loop=loop)

    def shutdown(self, config, state):
        """Shutdown clients"""
        loop = self.loop

        # Tell clients to shutdown
        ensure_future = asyncio.ensure_future
        t = [ensure_future(c.shutdown(config, state), loop=loop)
             for c in self._clients]
        f = asyncio.gather(*t, loop=loop)
        loop.run_until_complete(f)

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


class ProcessOptions:
    """Process cli options"""

    def __call__(self, config, args):
        llconfig = config['loadlimit']
        order = ['timezone', 'numusers', 'duration', 'taskimporter', 'tqdm',
                 'cache', 'export', 'periods', 'logging', 'verbose',
                 'qmaxsize', 'flushwait', 'initrate', 'schedsize',
                 'sched_delay']
        for name in order:
            getattr(self, name)(llconfig, args)

    def timezone(self, config, args):
        """Setup timezone config"""
        config['timezone'] = timezone(args.timezone)

    def numusers(self, config, args):
        """Setup number of users config"""
        numusers = args.numusers
        if numusers == 0:
            raise ValueError('users option expected value > 0, got {}'.
                             format(numusers))
        config['numusers'] = numusers

    def duration(self, config, args):
        """Setup duration config"""
        delta = Timedelta(args.duration)
        if not isinstance(delta, Timedelta):
            raise ValueError('duration option got invalid value {!r}'.
                             format(args.duration))
        config['duration'] = delta

    def taskimporter(self, config, args):
        """Setup task importer config"""
        config['importer'] = TaskImporter(*args.taskfile)

    def tqdm(self, config, args):
        """Setup tqdm config"""
        config['show-progressbar'] = args.progressbar

    def cache(self, config, args):
        """Setup cache config"""
        cache_type = args.cache
        config['cache'] = dict(type=cache_type)

    def export(self, config, args):
        """Setup export config"""
        config['export'] = exportsection = {}
        exportsection['type'] = export = args.export
        if export is not None:
            exportdir = args.exportdir
            if exportdir is None:
                exportdir = os.getcwd()
            if not isdir(exportdir):
                raise FileNotFoundError(exportdir)
            exportsection['targetdir'] = exportdir

    def periods(self, config, args):
        """Setup period config"""
        if args.periods <= 1:
            raise ValueError('periods option must be > 1')
        config['periods'] = args.periods

    def logging(self, config, args):
        """Setup logging config"""
        if args.uselogfile:
            logfile = args.logfile
            path = (Path.cwd() / '{}.log'.format(PROGNAME)
                    if logfile is None else Path(logfile))
            if not path.parent.is_dir():
                raise FileNotFoundError(str(path.parent))
            elif path.is_dir():
                raise IsADirectoryError(str(path))
            config['logging'] = {'logfile': str(path)}

    def verbose(self, config, args):
        """Setup verbosity config"""
        verbosity = 10 if args.verbosity >= 3 else (3 - args.verbosity) * 10
        logsection = config.setdefault('logging', {})
        loglevels = {l.value: l for l in LogLevel}
        logsection['loglevel'] = loglevels[verbosity]

    def qmaxsize(self, config, args):
        """Setup verbosity config"""
        config['qmaxsize'] = args.qmaxsize

    def flushwait(self, config, args):
        """Setup flushwait config"""
        try:
            delta = Timedelta(args.flushwait)
        except Exception:
            delta = None
        if not isinstance(delta, Timedelta):
            raise ValueError('duration option got invalid value: {}'.
                             format(args.flushwait))
        config['flushwait'] = delta

    def initrate(self, config, args):
        """Setup initrate config"""
        config['initrate'] = args.initrate

    def schedsize(self, config, args):
        """Setup schedsize config"""
        size = args.schedsize
        numusers = config['numusers']
        if size > numusers:
            msg = ('sched-size option expected maximum value of {}, '
                   'got value {}')
            raise ValueError(msg.format(numusers, size))
        config['schedsize'] = size

    def sched_delay(self, config, args):
        """Setup sched_delay config"""
        try:
            delta = Timedelta(args.sched_delay)
        except Exception:
            delta = None
        if not isinstance(delta, Timedelta):
            raise ValueError('sched-delay option got invalid value: {}'.
                             format(args.sched_delay))
        config['sched_delay'] = delta


process_options = ProcessOptions()


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

    # logging
    parser.add_argument(
        '-L', '--enable-logfile', dest='uselogfile', action='store_true',
        help='Enable logging to a logfile'
    )

    parser.add_argument(
        '-l', '--logfile', metavar='FILE', dest='logfile', default=None,
        help=('If logging to a file is enabled, log to FILE. Default: {}.log'.
              format(PROGNAME))
    )

    # Set loglevel
    parser.add_argument(
        '-v', '--verbose', dest='verbosity', action='count', default=0,
        help='Increase verbosity'
    )

    # Set maximum number of pending data
    parser.add_argument(
        '--pending-size', dest='qmaxsize', default=1000, type=int,
        help='Number of datapoints waiting to be worked on. Default: 1000'
    )

    # Set time to wait between flushes
    parser.add_argument(
        '--flush-wait', dest='flushwait', default='2s',
        help=('The amount of time to wait before flushing data to disk. '
              'Default: 2 seconds')
    )

    # Client init rate
    parser.add_argument(
        '--user-init-rate', dest='initrate', default=0, type=int,
        help=('The number of users to spawn every second. '
              'Default: 0 (ie spawn all users at once)')
    )

    # Client schedule rate
    parser.add_argument(
        '--sched-size', dest='schedsize', default=0, type=int,
        help=('The number of users to schedule at once. '
              'Default: 0 (ie schedule all users)')
    )

    parser.add_argument(
        '--sched-delay', dest='sched_delay', default='0s',
        help=('The amount of time to wait before scheduling the number of '
              'users defined by --sched-size. Default: 0 (ie schedule all '
              'users)')
    )

    parser.set_defaults(_main=RunLoop())


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
        self._countstore = state.countstore

        # Setup event list
        state.event = []

    def __enter__(self):
        config = self._config
        state = self._state
        llconfig = config['loadlimit']
        self._statsdict = statsdict = Period()
        countstore = self._countstore

        if llconfig['cache']['type'] == 'memory':
            self._calcobj = tuple(c(statsdict=statsdict, countstore=countstore)
                                  for c in [Total, TimeSeries, TotalError,
                                            TotalFailure])
            state.sqlengine = None
        else:
            cachefile = pathjoin(llconfig['tempdir'], 'cache.db')
            connstr = 'sqlite:///{}'.format(cachefile)
            state.sqlengine = engine = create_engine(connstr)
            self._calcobj = tuple(
                c(statsdict=statsdict, sqlengine=engine, countstore=countstore)
                for c in [SQLTotal, SQLTimeSeries, SQLTotalError,
                          SQLTotalFailure])

            # Subscribe to shutdown command
            channel.shutdown(partial(flushtosql_shutdown, statsdict=statsdict,
                                     sqlengine=engine))

            # Add flushtosql to timedata event
            stat.timedata(flushtosql)

        return self

    def __exit__(self, errtype, err, errtb):
        calcobj = self._calcobj
        total, timeseries, error, failure = calcobj
        countstore = self._countstore
        with ExitStack() as stack:
            # Set timeseries periods
            timeseries.vals.periods = self._config['loadlimit']['periods']

            if countstore.start_date is None:
                return

            # Add start and end events to the event timeline
            event = self._state.event
            event.insert(0, Event(EventType.start, countstore.start_date))
            event.append(Event(EventType.end, countstore.end_date))

            # Enter results contexts
            for r in calcobj:
                stack.enter_context(r)

            # Run calculations
            for name, data, err, fail in total:
                for r in calcobj:
                    r.calculate(name, data, err, fail)

        # Don't export
        exportconfig = self._config['loadlimit']['export']
        export_type = exportconfig['type']
        if export_type is None:
            results = (
                (total.vals.results, ) + timeseries.vals.results +
                (error.vals.results, failure.vals.results)
            )
            self._results = results
            return

        exportdir = exportconfig['targetdir']

        # Export values
        for r in calcobj:
            r.export(export_type, exportdir)

        # Capture any changes
        results = (
            (total.vals.results, ) + timeseries.vals.results +
            (error.vals.results, failure.vals.results)
        )
        self._results = results

    def startevent(self):
        """Start events"""
        countstore = self._countstore
        llconfig = self._config['loadlimit']
        qmaxsize = llconfig['qmaxsize']
        engine = self._state.sqlengine

        # Schedule SendTimeData
        flushwait = llconfig['flushwait']
        send = SendTimeData(countstore, flushwait=flushwait,
                            channel=stat.timedata)
        asyncio.ensure_future(send())

        # Assign shutdown tasks
        channel.shutdown(send.shutdown, anchortype=channel.AnchorType.first)
        channel.shutdown(stat.timedata.shutdown)
        channel.shutdown(cleanup.shutdown)

        # Start cleanup channel
        cleanup.add(TQDMCleanup(self._config, self._state))
        cleanup.open()
        cleanup.start()

        # Start timedata channel
        stat.timedata.open(maxsize=qmaxsize, cleanup=cleanup)
        stat.timedata.start(statsdict=self._statsdict, sqlengine=engine)

    @property
    def results(self):
        """Return stored results"""
        return self._results


class RunLoop:
    """Setup, run, and teardown loop"""

    def __init__(self):
        self._main = None
        self._statsetup = None

    def __call__(self, config, args, state):
        """Process cli options and start the loop"""
        self.init(config, args, state)
        stackorder = ['tempdir', 'statsetup', 'mainloop', 'mainloop_logging',
                      'initclients', 'setuptqdm', 'startmain',
                      'shutdown_clients']

        with ExitStack() as stack:
            pbar = stack.enter_context(
                tqdm_context(config, state, name=PROGNAME, desc='Progress',
                             total=len(stackorder)))
            for name in stackorder:
                func = getattr(self, name)
                if pbar is None:
                    state.write('{}: '.format(func.__doc__), end='')
                    state.write('', end='', flush=True)
                    time.sleep(0.1)
                func(stack, config, state)
                if pbar is None:
                    state.write('OK')
                else:
                    time.sleep(0.1)
                    pbar.update(1)

        ret = self._main.exitcode
        self._main = None
        self._statsetup = None
        state.write('\n\n', startnewline=True)
        return ret

    def init(self, config, args, state):
        """Initial setup"""
        llconfig = config['loadlimit']

        # Process cli options
        process_options(config, args)

        # Set up importhook
        sys.meta_path.append(llconfig['importer'])

        # Create state namespace
        state.reschedule = True
        state.progressbar = {}
        state.tqdm_progress = {}
        state.write = Printer()
        state.countstore = measure

        self._statsetup = StatSetup(config, state)

    def tempdir(self, stack, config, state):
        """Setup temporary directory"""
        config['loadlimit']['tempdir'] = abspath(
            stack.enter_context(TemporaryDirectory()))

    def statsetup(self, stack, config, state):
        """Setup stats recording"""
        stack.enter_context(self._statsetup)

    def mainloop(self, stack, config, state):
        """Create main loop"""
        loglevel = config['loadlimit']['logging']['loglevel']
        self._main = stack.enter_context(MainLoop(loglevel=loglevel,
                                                  clientcls=TQDMClient))

    def mainloop_logging(self, stack, config, state):
        """Setup main loop logging"""
        llconfig = config['loadlimit']
        logfile = llconfig.get('logging', {}).get('logfile', None)
        main = self._main
        main.initlogging(datefmt='%Y-%m-%d %H:%M:%S.%f',
                         style='{',
                         format='{asctime} {levelname} {name}: {message}',
                         fmtcls=LoadLimitFormatter, tz=llconfig['timezone'],
                         logfile=logfile)

    def initclients(self, stack, config, state):
        """Create and initialize clients"""
        numusers = config['loadlimit']['numusers']
        with tqdm_context(config, state, name='init', desc='Ramp-up',
                          total=numusers):
            self._main.init(config, state)

    def setuptqdm(self, stack, config, state):
        """Setup tqdm progress bars"""
        duration = int(config['loadlimit']['duration'].total_seconds())
        stack.enter_context(tqdm_context(config, state, name='runtime',
                                         total=duration, desc='Run time',
                                         sched=True))
        stack.enter_context(tqdm_context(config, state, name='iteration',
                                         desc='Iterations'))
        stack.enter_context(tqdm_context(config, state, name='cleanup',
                                         desc='Cleanup'))

    def startmain(self, stack, config, state):
        """Start the main loop"""
        # Start events
        self._statsetup.startevent()

        # Start the loop
        self._main.start()

    def shutdown_clients(self, stack, config, state):
        """Tell clients to shutdown"""
        numusers = config['loadlimit']['numusers']
        with tqdm_context(config, state, name='shutdown',
                          desc='Stopping Clients', total=numusers):
            self._main.shutdown(config, state)


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
