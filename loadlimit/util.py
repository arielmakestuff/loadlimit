# -*- coding: utf-8 -*-
# loadlimit/util.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Utility objects and functions"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import argparse
from datetime import datetime, timezone
from enum import Enum
import logging

# Third-party imports

# Local imports


# ============================================================================
# Globals
# ============================================================================


LogLevel = Enum('LogLevel', [(k, v) for k, v in logging._nameToLevel.items()
                             if k not in ['WARN', 'NOTSET']])


TZ_UTC = timezone.utc


# ============================================================================
# Date utils
# ============================================================================


def now(tzinfo=None):
    """Generate the current datetime.

    Defaults to UTC timezone.

    """
    tzinfo = TZ_UTC if tzinfo is None else tzinfo
    return datetime.now(tzinfo)


# ============================================================================
# Namespace
# ============================================================================


class Namespace(argparse.Namespace):
    """Namespace extended with bool check

    The bool check is to report whether the namespace is empty or not

    """

    def __bool__(self):
        """Return True if attributes are being stored"""
        return self != self.__class__()


# ============================================================================
#
# ============================================================================
