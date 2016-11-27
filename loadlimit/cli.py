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
from importlib import import_module
import sys

# Third-party imports
from pandas import Timedelta
from pytz import timezone

# Local imports
from . import event
from .core import BaseLoop, Client
from .importhook import TaskImporter
from .util import aiter


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
# MainLoop
# ============================================================================


class MainLoop(BaseLoop):
    """Integrates with Client"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._clients = frozenset()

    def init(self, config):
        """Initialize clients"""
        clients = frozenset(self.spawn_clients(config))
        self._clients = clients
        loop = self.loop

        # Init clients
        ensure_future = asyncio.ensure_future
        t = [ensure_future(c.init(config), loop=loop) for c in clients]
        f = asyncio.gather(*t, loop=loop)
        loop.run_until_complete(f)

        # Schedule loop end
        ensure_future(self.endloop(config), loop=loop)

        # Schedule clients on the loop
        for c in clients:
            ensure_future(c(), loop=loop)

    def spawn_clients(self, config):
        """Spawns clients according the given config"""
        taskmod = import_module('loadlimit.task')
        tasklist = taskmod.__tasks__
        numclients = config['loadlimit']['numusers']
        tasks = [Client(tasklist, reschedule=True) for _ in range(numclients)]
        return tasks

    async def endloop(self, config):
        """coro func that ends the loop after a given duration"""
        duration = config['loadlimit']['duration']
        await asyncio.sleep(duration.total_seconds())

        # Stop rescheduling clients
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


def runloop(config, args):
    """Process cli options and start the loop"""

    # Process cli options
    process_options(config, args)

    # Set up importhook
    sys.meta_path.append(config['loadlimit']['importer'])

    # Run the loop
    with MainLoop() as main:

        # Create and initialize clients
        main.init(config)

        # Start the loop
        main.start()

    return main.exitcode


# ============================================================================
# Main
# ============================================================================


def main(arglist=None, config=None):
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

    parser = create()
    args = parser.parse_args(arglist)
    exitcode = args._main(config, args)
    sys.exit(exitcode)


if __name__ == '__main__':
    main()


# ============================================================================
#
# ============================================================================
