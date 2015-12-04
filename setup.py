#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import glob
from distutils.core import setup

PACKAGES = ['triton']


def get_init_val(val, packages=PACKAGES):
    pkg_init = "%s/__init__.py" % PACKAGES[0]
    value = '__%s__' % val
    fn = open(pkg_init)
    for line in fn.readlines():
        if line.startswith(value):
            return line.split('=')[1].strip().strip("'")


setup(
    name='py-%s' % get_init_val('title'),
    version=get_init_val('version'),
    description=get_init_val('description'),
    long_description=open('README.md').read(),
    author=get_init_val('author'),
    author_email=get_init_val('author_email'),
    url=get_init_val('url'),
    scripts=glob.glob("bin/triton*"),
    install_requires=['python-snappy', 'msgpack_python', 'pyzmq'],
    packages=PACKAGES
)
