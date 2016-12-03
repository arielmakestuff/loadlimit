# -*- coding: utf-8 -*-
# test/unit/importhook/test_taskimporter.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test taskfilematch()"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from os.path import splitext
from random import choice
import sys
from importlib import import_module

# Third-party imports
import pytest

# Local imports
from loadlimit.core import TaskABC
import loadlimit.importhook
from loadlimit.importhook import TaskImporter, mkmodule


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def modpath():
    """Return the virtual task python path"""
    return 'loadlimit.task'


@pytest.yield_fixture
def cleanup(modpath):
    """Remove dummy modules from sys.modules"""
    yield

    # Remove dummy modules from sys.modules
    pathlist = [p for p in sys.modules
                if p != modpath and p.startswith(modpath)]

    for p in pathlist:
        del sys.modules[p]

    if modpath in sys.modules:
        del sys.modules[modpath]

    # Remove TaskImporter
    index = [i for i, obj in enumerate(sys.meta_path)
             if isinstance(obj, TaskImporter)]

    for i in reversed(index):
        assert isinstance(sys.meta_path[i], TaskImporter)
        sys.meta_path.pop(i)


# ============================================================================
# Auto-fixture
# ============================================================================


pytestmark = pytest.mark.usefixtures('cleanup')


# ============================================================================
# Helpers
# ============================================================================


class FakeSourceFileLoader:
    """Fake SourceFileLoader"""

    def __init__(self, name, path):
        self._fake_name = name

    def load_module(self, name):
        """Fake load_module"""
        src = self.mksrc()
        return mkmodule(src, name)

    def exec_module(self, module):
        """Fake exec_module"""
        src = self.mksrc()
        c = compile(src, '', 'exec')
        exec(c, module.__dict__)

    def mksrc(self):
        """Make source code"""
        src = """
        TEST = '{}'
        """.format(self._fake_name).strip()
        return src


class FakeModuleWithTasks(FakeSourceFileLoader):
    """Create module with tasks"""

    def mksrc(self):
        """Make source code"""
        src = """
from loadlimit.core import TaskABC

class TestTask(TaskABC):

    def __init__(self):
        self._val = None

    async def __call__(self, state):
        self._val.append(42)

    async def init(self, config, state):
        self._val = config['val']

    async def shutdown(self, config, state):
        pass

class JustASimpleClass:
    pass
        """.strip()
        return src


def fake_lstaskfiles(*taskfiles, taskdir=None, checkerr=False):
    """Files"""
    ret = [tuple(splitext(t)) + (t, ) for t in taskfiles]
    return ret


# ============================================================================
# Test no tasks
# ============================================================================


def test_taskmod_no_taskfile(modpath):
    """Create virtual task module when given no task files"""
    sys.meta_path.append(TaskImporter())
    task = import_module(modpath)
    assert modpath in sys.modules
    assert sys.modules[modpath] is task
    assert task.__taskmodules__ == []


# ============================================================================
# Test test files only
# ============================================================================


def test_taskmod_taskfiles_only(monkeypatch, modpath):
    """Create virtual task module and task files"""

    monkeypatch.setattr(loadlimit.importhook, 'lstaskfiles', fake_lstaskfiles)
    monkeypatch.setattr(loadlimit.importhook, 'SourceFileLoader',
                        FakeSourceFileLoader)

    taskfiles = ['a_{}.py'.format(i) for i in range(10)]
    names = [splitext(n)[0] for n in taskfiles]
    pypath = ['{}.{}'.format(modpath, n) for n in names]

    sys.meta_path.append(TaskImporter(*taskfiles))
    task = import_module(modpath)

    assert modpath in sys.modules
    assert sys.modules[modpath] is task
    assert task.__taskmodules__ == pypath
    for n in names:
        assert hasattr(task, n)
        assert getattr(task, n).TEST == '{}.{}'.format(modpath, n)


# ============================================================================
# Test import task files
# ============================================================================


def test_taskfile_import(monkeypatch, modpath):
    """Importing taskfile loads taskmod and all other task files"""
    monkeypatch.setattr(loadlimit.importhook, 'lstaskfiles', fake_lstaskfiles)
    monkeypatch.setattr(loadlimit.importhook, 'SourceFileLoader',
                        FakeSourceFileLoader)

    taskfiles = ['a_{}.py'.format(i) for i in range(10)]
    names = [splitext(n)[0] for n in taskfiles]
    pypath = ['{}.{}'.format(modpath, n) for n in names]
    randpath = choice(pypath)

    assert modpath not in sys.modules
    assert all(not p.startswith(modpath) for p in sys.modules)

    sys.meta_path.append(TaskImporter(*taskfiles))
    taskfile = import_module(randpath)

    expected = set(pypath) | set([modpath])
    result = set(p for p in sys.modules if p.startswith(modpath))

    assert modpath in sys.modules
    assert result == expected
    assert taskfile.TEST == randpath


def test_taskfile_taskmod_loaded(monkeypatch, modpath):
    """Import taskfile when taskmod already loaded"""
    monkeypatch.setattr(loadlimit.importhook, 'lstaskfiles', fake_lstaskfiles)
    monkeypatch.setattr(loadlimit.importhook, 'SourceFileLoader',
                        FakeSourceFileLoader)

    taskfiles = ['a_{}.py'.format(i) for i in range(10)]
    names = [splitext(n)[0] for n in taskfiles]
    pypath = ['{}.{}'.format(modpath, n) for n in names]
    randpath = choice(pypath)

    sys.meta_path.append(TaskImporter(*taskfiles))
    import_module(modpath)

    # Forcibly remove the generated taskfile
    sys.modules.pop(randpath)

    import_module(randpath)


# ============================================================================
# Test Not handled import
# ============================================================================


def test_import_nothandled():
    """Raise ImportError for unhandled imports"""
    sys.meta_path.append(TaskImporter())
    with pytest.raises(ImportError):
        import_module('not.exist')


# ============================================================================
# Test multiple imports
# ============================================================================


@pytest.mark.parametrize('numtimes', [choice(list(range(10)))])
def test_import_multiple(monkeypatch, modpath, numtimes):
    """Importing the same module 2+ times returns original created module"""
    monkeypatch.setattr(loadlimit.importhook, 'lstaskfiles', fake_lstaskfiles)
    monkeypatch.setattr(loadlimit.importhook, 'SourceFileLoader',
                        FakeSourceFileLoader)

    taskfiles = ['a_{}.py'.format(i) for i in range(10)]
    names = [splitext(n)[0] for n in taskfiles]
    pypath = ['{}.{}'.format(modpath, n) for n in names]
    randpath = choice(pypath)

    sys.meta_path.append(TaskImporter(*taskfiles))
    taskfile = import_module(randpath)

    for i in range(numtimes):
        newmod = import_module(randpath)
        assert newmod is taskfile


# ============================================================================
# Test mkmodule()
# ============================================================================


def test_mkmodule():
    """docstring for test_tmp2"""
    src = """
    def foo():
        return 'BAR'
    """.strip()
    m = mkmodule(src, 'hello')
    assert hasattr(m, 'foo')
    assert m.foo() == 'BAR'


# ============================================================================
# Test find task coroutines
# ============================================================================


def test_findtasks_none(monkeypatch, modpath):
    """No tasks found has empty __tasks__ attr"""
    monkeypatch.setattr(loadlimit.importhook, 'lstaskfiles', fake_lstaskfiles)
    # monkeypatch.setattr(loadlimit.importhook, 'SourceFileLoader',
    #                     FakeModuleWithTasks)
    monkeypatch.setattr(loadlimit.importhook, 'SourceFileLoader',
                        FakeSourceFileLoader)

    taskfile = 'a_0.py'

    sys.meta_path.append(TaskImporter(taskfile))
    taskmod = import_module(modpath)

    assert hasattr(taskmod, '__tasks__')
    assert taskmod.__tasks__ == []


def test_findtasks_found(monkeypatch, modpath):
    """Find tasks in task files and add to loadlimit.task.__tasks__"""
    monkeypatch.setattr(loadlimit.importhook, 'lstaskfiles', fake_lstaskfiles)
    monkeypatch.setattr(loadlimit.importhook, 'SourceFileLoader',
                        FakeModuleWithTasks)

    taskfile = 'a_0.py'

    sys.meta_path.append(TaskImporter(taskfile))
    taskmod = import_module(modpath)

    assert len(taskmod.__tasks__) == 1
    task = taskmod.__tasks__[0]
    assert task.__name__ == 'TestTask'
    assert isinstance(task, type)
    assert issubclass(task, TaskABC)


# ============================================================================
#
# ============================================================================
