#!/usr/bin/env python3
# coding=utf-8
"""
Copyright Samuel Lloyd
28/06/2020
samueljohnlloyd12@gmail.com

Parameters
----------

Return
------
"""

import pytest

from pathlib import Path

from templateproject import *

test_path = Path(__file__)


@pytest.fixture(scope="module")
def fixture():
    yield


@pytest.mark.usefixtures("fixture")
class Test:
    pass
