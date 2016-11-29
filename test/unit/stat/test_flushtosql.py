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
from hashlib import sha1

# Third-party imports
from pandas import DataFrame, read_sql_table, to_timedelta
import pytest
from sqlalchemy import create_engine

# Local imports
from loadlimit.core import BaseLoop
import loadlimit.event as event
from loadlimit.event import NoEventTasksError
import loadlimit.stat as stat
from loadlimit.stat import flushtosql, flushtosql_shutdown, timecoro, Total
from loadlimit.util import aiter


# ============================================================================
# Fixtures
# ============================================================================


pytestmark = pytest.mark.usefixtures('fake_shutdown_event',
                                     'fake_recordperiod_event')


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

    results = Total()

    # Create coro to time
    @timecoro(name='churn')
    async def churn(i):
        """Do nothing"""
        await asyncio.sleep(0)

    async def run():
        """run"""
        async for i in aiter(range(num)):
            await churn(i)
        event.shutdown.set(exitcode=0)

    # Add to shutdown event
    event.shutdown(partial(flushtosql_shutdown, statsdict=results.statsdict,
                           sqlengine=engine))

    # Run all the tasks
    with BaseLoop() as main:

        # Add flushtosql to recordperiod event
        stat.recordperiod(flushtosql, schedule=False)

        # Start every event, and ignore events that don't have any tasks
        stat.recordperiod.start(ignore=NoEventTasksError, reschedule=True,
                                statsdict=results.statsdict, flushlimit=5,
                                sqlengine=engine)

        asyncio.ensure_future(run())
        main.start()

    assert results.statsdict.numdata == 0
    with engine.begin() as conn:
        name = sha1('churn'.encode('utf-8')).hexdigest()
        name = 'period_{}'.format(name)
        df = read_sql_table(name, conn, index_col='index')

    assert results.statsdict.total() == 0
    df['delta'] = df['delta'].apply(partial(to_timedelta, unit='ns'))

    assert isinstance(df, DataFrame)
    assert not df.empty
    assert len(df) == num


# ============================================================================
#
# ============================================================================
