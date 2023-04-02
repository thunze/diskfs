"""Tests for the ``darwin`` module."""

import sys
from typing import TYPE_CHECKING

import pytest

# Make mypy and pytest collection logic happy
if not TYPE_CHECKING and sys.platform != 'darwin':
    pytest.skip('Skipping Darwin-only tests', allow_module_level=True)
assert sys.platform == 'darwin'


from diskfs.base import DeviceProperties

# noinspection PyProtectedMember
from diskfs.darwin import (
    _releasing,
    _unpack_cf_boolean,
    _unpack_cf_string,
    device_properties,
)


def test__releasing_fail():
    """Test that the context manager ``_releasing()`` raises ``ValueError`` when
    ``None`` is passed.
    """
    with pytest.raises(ValueError):
        with _releasing(None):
            pass


def test__unpack_cf_boolean_none():
    """Test that ``_unpack_cf_boolean()`` returns ``None`` when ``None`` is passed."""
    assert _unpack_cf_boolean(None) is None


def test__unpack_cf_string_none():
    """Test that ``_unpack_cf_string()`` returns ``None`` when ``None`` is passed."""
    assert _unpack_cf_string(None) is None


def test_device_properties_all_none(tempfile):
    """Test that ``device_properties()`` returns ``(None, None, None)`` when a Disk
    Arbitration description of ``file`` cannot be obtained via
    ``DADiskCopyDescription()``.

    This is for example the case when Disk Arbitration is contacted based on a file
    object for a regular file instead of a block device.
    """
    with tempfile.open('rb') as f:
        assert device_properties(f) == DeviceProperties(None, None, None)
