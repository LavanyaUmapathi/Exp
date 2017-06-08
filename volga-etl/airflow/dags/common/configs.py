#!/usr/bin/env python
from __future__ import print_function

import os
from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


BASE_DIR = os.path.normpath(
    os.path.join(
        os.path.split(os.path.abspath(__file__))[0],
        '..', '..', 'configs'))


def get_config(name):
    config_path = os.path.join(BASE_DIR, name)
    with open(config_path) as cfile:
        config = load(cfile.read(), Loader=Loader)
    return config
