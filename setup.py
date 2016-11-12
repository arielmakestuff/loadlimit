#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Setup file for loadlimit."""

import sys
from setuptools import setup


def setup_package():
    needs_sphinx = {'build_sphinx', 'upload_docs'}.intersection(sys.argv)
    sphinx = ['sphinx'] if needs_sphinx else []
    setup(setup_requires=['pbr>=1.9', 'setuptools>=17.1'] + sphinx, pbr=True)


if __name__ == "__main__":
    setup_package()
