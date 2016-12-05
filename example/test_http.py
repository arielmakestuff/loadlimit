# -*- coding: utf-8 -*-
# /home/smokybobo/opt/repos/git/personal/loadlimit/tmp/example/loadlimit/test_http.py
# Copyright (C) 2016 authors and contributors (see AUTHORS file)
#
# This module is released under the MIT License.

"""Test AIOHTTP task"""

# ============================================================================
# Imports
# ============================================================================


# Stdlib imports
import asyncio
from asyncio import Queue
from collections import OrderedDict
from itertools import count
import json
from pathlib import Path
import re

# Third-party imports
import aiohttp
# from aiohttp import ClientResponseError, ServerDisconnectedError
from haralyzer import HarParser

# Local imports
from loadlimit.core import TaskABC
from loadlimit.util import aiter
from loadlimit.stat import Failure, timecoro


# ============================================================================
# Globals
# ============================================================================


HARFILE = Path.cwd() / 'httprequests.har'


# ============================================================================
# Helpers
# ============================================================================


def mkparser(filename):
    """docstring for mkparser"""
    with open(filename, encoding='utf-8') as f:
        parser = HarParser(json.loads(f.read(), encoding='utf-8',
                                      object_hook=OrderedDict))
    return parser


# ============================================================================
# HttpTask
# ============================================================================


class HttpTask(TaskABC):
    """HttpTask"""

    parser = None
    session = None
    queue = None

    whitelist = [
        re.compile(regex) for regex in [
            r'http[s]?://www[.]worldvision[.]ca'
        ]
    ]

    blacklist = [
        re.compile(regex) for regex in [
        ]
    ]

    def __init__(self):
        """docstring for __init__"""
        cls = self.__class__
        self._queue = None
        self.urls = None

        # Get har file
        if cls.parser is None:
            cls.parser = mkparser(str(HARFILE))

    async def callmethod(self, method, url, kwargs):
        """docstring for fetch"""
        if method == 'post':
            return
        cls = self.__class__
        session = cls.session
        queue = cls.queue
        name = url

        @timecoro(name='{} {}'.format(method, name))
        async def runmethod():
            """docstring for runmethod"""
            await queue.get()
            func = getattr(session, method)
            async with func(url, **kwargs) as response:
                await response.read()
                queue.task_done()
                if response.status >= 400:
                    msg = ('ERROR {} {}: {}'.format(method, url,
                                                    response.status))
                    raise Failure(msg)
                # raise RuntimeError('WHAT DOES THE FOX SAY')

        await queue.put(True)
        try:
            await runmethod()
        except asyncio.CancelledError:
            pass

    async def __call__(self, state):
        tasks = []
        async for urls in aiter(self.urls.values()):
            async for method, url, kwargs in aiter(urls):
                coro = self.callmethod(method, url, kwargs)
                tasks.append(asyncio.ensure_future(coro))
            asyncio.sleep(0)

        if tasks:
            await asyncio.gather(*tasks)

    # --------------------
    # init methods
    # --------------------

    async def init(self, config, state):
        """docstring for init"""
        cls = self.__class__
        if cls.queue is None:
            cls.queue = Queue()
        self.urls = await self.mkurls()
        if cls.session is None:
            cls.session = aiohttp.ClientSession()

    async def matchurl(self, url, regexlist):
        """docstring for checklist"""
        async for regex in aiter(regexlist):
            if regex.match(url):
                return True
            asyncio.sleep(0)
        return False

    async def mkurls(self):
        """docstring for mkurls"""
        cls = self.__class__
        parser = cls.parser
        counter = count()
        urls = OrderedDict()
        async for page in aiter(parser.pages):
            pagekey = next(counter)
            pageurls = urls.setdefault(pagekey, [])
            async for entry in aiter(page.entries):
                request = entry['request']
                method = request['method'].lower()
                url = request['url']
                if not await self.matchurl(url, cls.whitelist):
                    asyncio.sleep(0)
                    continue
                elif await self.matchurl(url, cls.blacklist):
                    asyncio.sleep(0)
                    continue
                headers = {h['name']: h['value'] for h in request['headers']
                           if h['name'].lower() != 'cookie'}
                kwargs = dict(headers=headers)
                if method == 'post':
                    postdata = request.get('postData', None)
                    if postdata:
                        data = {p['name']: p['value'] for p in
                                postdata.get('params', [])}
                        if data:
                            kwargs.update(data=data)
                pageurls.append((method, url, kwargs))
                asyncio.sleep(0)
            asyncio.sleep(0)

        return urls

    # --------------------
    # Shutdown methods
    # --------------------
    async def shutdown(self, config, state):
        cls = self.__class__
        queue = cls.queue
        if queue is not None:
            await queue.join()
            cls.queue = None
        if cls.session is not None:
            cls.session.close()
            cls.session = None


# ============================================================================
#
# ============================================================================
