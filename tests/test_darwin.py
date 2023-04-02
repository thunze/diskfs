"""Tests for the ``darwin`` module."""

import sys
from typing import TYPE_CHECKING

import pytest

# Make mypy and pytest collection logic happy
if not TYPE_CHECKING and sys.platform != 'darwin':
    pytest.skip('Skipping Darwin-only tests', allow_module_level=True)
assert sys.platform == 'darwin'
