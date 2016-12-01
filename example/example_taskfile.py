# Module:
# Submodules:
# Created:
# Copyright (C) <date> <fullname>
#
# This module is part of the <project name> project and is released under
# the MIT License: http://opensource.org/licenses/MIT
"""
"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio

# Third-party imports
from loadlimit.core import TaskABC
from loadlimit.stat import timecoro

# Local imports


# ============================================================================
# Example
# ============================================================================


@timecoro(name='measureme')
async def measureme():
    """docstring for measureme"""
    asyncio.sleep(0.1)


class Example(TaskABC):

    async def __call__(self, state):
        """docstring for __call__"""
        await measureme()

    async def init(self, config, state):
        """docstring for init"""


# ============================================================================
#
# ============================================================================
