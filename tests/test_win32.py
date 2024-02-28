"""Tests for the ``win32`` module."""

import sys
from typing import TYPE_CHECKING

import pytest

# Make mypy and pytest collection logic happy
if not TYPE_CHECKING and sys.platform != "win32":
    pytest.skip("Skipping Windows-only tests", allow_module_level=True)
assert sys.platform == "win32"


# noinspection PyProtectedMember
from diskfs.win32 import IOCTL_STORAGE_QUERY_PROPERTY, device_io_control


def test_device_io_control_fail(tempfile):
    """Test that ``device_io_control()`` raises an ``OSError`` with the ``winerror``
    attribute set when called with an invalid combination of arguments for
    ``DeviceIoControl()``.

    In this case, the control code ``IOCTL_STORAGE_QUERY_PROPERTY`` expects an input
    buffer to be passed to ``DeviceIoControl()`` which we do not provide. Also,
    the file handle passed to ``DeviceIoControl()`` must be one of a block device; we
    pass ``tempfile`` instead.
    """
    with tempfile.open("rb") as f, pytest.raises(OSError) as exc_info:
        device_io_control(f.fileno(), IOCTL_STORAGE_QUERY_PROPERTY)
        assert exc_info.value.winerror is not None
