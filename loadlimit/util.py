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
from enum import Enum
import logging

# Third-party imports

# Local imports


# ============================================================================
# Globals
# ============================================================================


LogLevel = Enum('LogLevel', [(k, v) for k, v in logging._nameToLevel.items()
                             if k not in ['WARN', 'NOTSET']])


# ============================================================================
#
# ============================================================================
