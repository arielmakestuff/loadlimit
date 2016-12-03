# -*- coding: utf-8 -*-
# test/unit/stat/test_flushtosql.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test flushtosql()"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from functools import partial

# Third-party imports
from pandas import DataFrame
import pytest
from sqlalchemy import create_engine

# Local imports
import loadlimit.channel as channel
from loadlimit.core import BaseLoop
from loadlimit.event import NoEventTasksError
import loadlimit.stat as stat
from loadlimit.stat import (flushtosql, flushtosql_shutdown, SQLTotal,
                            timecoro)
from loadlimit.util import aiter


# ============================================================================
# Fixtures
# ============================================================================


pytestmark = pytest.mark.usefixtures('fake_shutdown_channel',
                                     'fake_recordperiod_channel')


# ============================================================================
# Tests
# ============================================================================


@pytest.mark.parametrize('num', [10, 12])
def test_flushtosql(num):
    """updateperiod updates statsdict with timeseries data points

    num fixture allows testing the flushtosql_shutdown coro func for:

    * all data has already been flushed to the sql db
    * there's still some data remaining that needs to be flushed to sql db

    """

    # Setup sqlalchemy engine
    engine = create_engine('sqlite://')

    timedata = SQLTotal(sqlengine=engine)

    # Create coro to time
    @timecoro(name='churn')
    async def churn(i):
        """Do nothing"""
        await asyncio.sleep(0)

    async def run():
        """run"""
        async for i in aiter(range(num)):
            await churn(i)
        await stat.recordperiod.join()
        await channel.shutdown.send(0)

    # Add to shutdown channel
    channel.shutdown(partial(flushtosql_shutdown, statsdict=timedata.statsdict,
                             sqlengine=engine))
    channel.shutdown(stat.recordperiod.shutdown)

    # Add flushtosql to recordperiod event
    stat.recordperiod(flushtosql)

    # Run all the tasks
    with BaseLoop() as main:

        # Start every event, and ignore events that don't have any tasks
        stat.recordperiod.open()
        stat.recordperiod.start(asyncfunc=False,
                                statsdict=timedata.statsdict, flushlimit=5,
                                sqlengine=engine)
        asyncio.ensure_future(run())
        main.start()

    assert timedata.statsdict.numdata == 0

    df = timedata()

    assert isinstance(df, DataFrame)
    assert not df.empty


# ============================================================================
#
# ============================================================================
