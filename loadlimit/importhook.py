# -*- coding: utf-8 -*-
# loadlimit/importhook.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Create virtual namespace loadlimit.task"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from importlib.machinery import ModuleSpec, SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from itertools import chain
from os import listdir
from os.path import isdir, isfile, join as pathjoin, split as splitpath
import re
import sys

# Third-party imports

# Local imports
from .core import TaskABC


# ============================================================================
# Globals
# ============================================================================


taskfile_regex = re.compile(r'^([\w_][\w\d_]+)([.]py)$')


# ============================================================================
# Helpers
# ============================================================================


def mkmodule(src, modname):
    """Create module from source string"""
    c = compile(src, '', 'exec')
    modspec = ModuleSpec(name=modname, loader=None)
    module = module_from_spec(modspec)
    exec(c, module.__dict__)
    return module


# ============================================================================
#
# ============================================================================


def taskfilematch(filename):
    """Filter out invalid or disabled runfiles"""
    if filename.endswith('.disabled') or not isfile(filename):
        return None
    d, name = splitpath(filename)
    return taskfile_regex.match(name)


def lstaskfiles(*taskfiles, taskdir=None, checkerr=True):
    """List valid task files"""
    if taskdir and not isdir(taskdir):
        errmsg = 'dir not found: {}'.format(taskdir)
        raise FileNotFoundError(errmsg)

    dirfiles = [] if not taskdir else listdir(taskdir)
    itermatch = ((p, taskfilematch(p)) for p in
                 chain(taskfiles,
                       (pathjoin(taskdir, f) for f in dirfiles)))
    return [(m.group(1), m.group(2), p) for p, m in itermatch if m]


# ============================================================================
# Metadata
# ============================================================================


class TaskImporter:
    """Metahook to create modules from task runfiles"""

    def __init__(self, *taskfiles, taskdir=None):
        """Initialize with taskdir"""
        self.taskfiles = taskfiles
        self.taskdir = taskdir
        self.modpath = 'loadlimit.task'

    def find_spec(self, fullname, path, target=None):
        """Only import loadlimit.task namespaces"""
        modpath = self.modpath
        modspec = None
        if fullname == modpath:
            modspec = ModuleSpec(name=fullname, loader=self, is_package=True)
        elif fullname.startswith(modpath):
            modspec = ModuleSpec(name=fullname, loader=self)
        return modspec

    def exec_module(self, module):
        """Execute module in namespace

        Do nothing since module already created.

        """
        modname = module.__spec__.name
        if modname == self.modpath:
            self.create_task_module(module)

    def create_module(self, spec):
        """Create module from spec

        Uses default process

        """

    def create_task_module(self, module):
        """Create the loadlimit.task module"""
        taskfiles = sorted(lstaskfiles(*self.taskfiles, taskdir=self.taskdir,
                                       checkerr=False),
                           key=lambda e: e[0])

        # Set some important attributes
        module.__taskmodules__ = taskmodules = []
        module.__tasks__ = tasks = []

        # Load taskfiles
        for name, ext, filepath in taskfiles:
            modname = '.'.join(['loadlimit', 'task', name])
            loader = SourceFileLoader(modname, filepath)
            spec = spec_from_loader(modname, loader)
            taskmod = module_from_spec(spec)
            loader.exec_module(taskmod)

            sys.modules[modname] = taskmod
            taskmodules.append(modname)
            setattr(module, name, taskmod)

            # Find all task objects
            for obj in vars(taskmod).values():
                if not isinstance(obj, type) or obj is TaskABC:
                    continue
                elif issubclass(obj, TaskABC):
                    tasks.append(obj)

        return module


# ============================================================================
#
# ============================================================================
