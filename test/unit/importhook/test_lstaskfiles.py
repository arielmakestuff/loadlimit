# -*- coding: utf-8 -*-
# test/unit/importhook/test_lstaskfiles.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test lstaskfiles()"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
from os.path import splitext
from random import choice

# Third-party imports
import pytest

# Local imports
import loadlimit.importhook


# ============================================================================
# Globals
# ============================================================================


lstaskfiles = loadlimit.importhook.lstaskfiles


# ============================================================================
# Test taskfilematch
# ============================================================================


@pytest.mark.parametrize('dirname', ['hello', 'world.py'])
def test_nodir(monkeypatch, dirname):
    """Raises FileNotFoundError is dirname is not a dir"""

    def fake_isdir(filename):
        return False

    monkeypatch.setattr(loadlimit.importhook, 'isdir', fake_isdir)

    expected = 'dir not found: {}'.format(dirname)
    with pytest.raises(FileNotFoundError) as err:
        lstaskfiles(taskdir=dirname)

    assert err.value.args == (expected, )


@pytest.mark.parametrize('filename', [
    '{}{}{}.py'.format(n, s, d) for n in 'abcde'
    for s in ['', '_']
    for d in [1, 10, 100]
])
def test_nofile(monkeypatch, filename):
    """Returns empty list if none of the files exist"""

    def fake_listdir(d):
        return ['{}.py'.format(filename)]

    def fake_isdir(d):
        return True

    monkeypatch.setattr(loadlimit.importhook, 'listdir', fake_listdir)
    monkeypatch.setattr(loadlimit.importhook, 'isdir', fake_isdir)

    expected = []
    result = lstaskfiles(taskdir='hello')


def test_some_nofile(monkeypatch):
    """Returns list of filenames that exist and match the regex"""

    def fake_listdir(d):
        ret = ['{}{}{}.py'.format(n, s, num) for n in 'abcde'
               for s in ['', '_']
               for num in [1, 10, 100]]
        return ret

    def fake_isfile(f):
        return False if f.endswith('10.py') else True

    def fake_isdir(d):
        return True

    monkeypatch.setattr(loadlimit.importhook, 'listdir', fake_listdir)
    monkeypatch.setattr(loadlimit.importhook, 'isfile', fake_isfile)
    monkeypatch.setattr(loadlimit.importhook, 'isdir', fake_isdir)

    expected = [splitext(f)[0] for f in fake_listdir('')
                if not f.endswith('10.py')]

    result = lstaskfiles(taskdir='hello')
    assert result
    assert all(not f[0].endswith('10') for f in result)
    assert [r[0] for r in result] == expected


# ============================================================================
# Test regex
# ============================================================================


@pytest.mark.parametrize('pattern', [
    'aa.py',
    '_a.py',
    'a1.py',
    'a10.py',
    'a10a10a.py'
])
def test_taskfile_regex(pattern):
    """Regex matches as expected"""
    assert loadlimit.importhook.taskfile_regex.match(pattern)


# ============================================================================
#
# ============================================================================
