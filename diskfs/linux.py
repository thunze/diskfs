"""Platform-specific disk operations for Linux systems."""

from __future__ import annotations

import sys

assert sys.platform == 'linux'  # skipcq: BAN-B101

import io
import os
from ctypes import c_uint
from fcntl import ioctl
from pathlib import Path
from typing import BinaryIO

from .base import DeviceProperties, SectorSize

__all__ = ['device_size', 'device_sector_size', 'reread_partition_table']


BLKSSZGET = 0x1268
BLKPBSZGET = 0x127B
BLKRRPART = 0x125F


def device_properties(file: BinaryIO) -> DeviceProperties:
    """Return additional properties of a block device.

    :param file: IO handle for the block device.
    """
    device = os.fstat(file.fileno()).st_rdev
    major, minor = os.major(device), os.minor(device)
    sysfs_base = Path(f'/sys/dev/block/{major}:{minor}')

    def read_text_or_none(path: Path) -> str | None:
        """Return text of file at ``path`` and ``rstrip()`` the resulting text or
        return ``None`` if ``path`` is not a file.
        """
        try:
            return path.read_text(encoding='utf-8').rstrip()
        except FileNotFoundError:
            return None

    removable_str = read_text_or_none(sysfs_base / 'removable')
    vendor = read_text_or_none(sysfs_base / 'device' / 'vendor')
    model = read_text_or_none(sysfs_base / 'device' / 'model')

    removable = None
    if removable_str is not None:
        try:
            removable = bool(int(removable_str))
        except ValueError:  # pragma: no cover
            pass

    return DeviceProperties(removable, vendor, model)


def device_size(file: BinaryIO) -> int:
    """Return the size of a block device.

    :param file: IO handle for the block device.
    """
    return file.seek(0, io.SEEK_END)


def device_sector_size(file: BinaryIO) -> SectorSize:
    """Return the logical and physical sector size of a block device.

    :param file: IO handle for the block device.
    """
    logical, physical = c_uint(), c_uint()  # see blkdev.h
    ioctl(file, BLKSSZGET, logical)
    ioctl(file, BLKPBSZGET, physical)
    return SectorSize(logical.value, physical.value)


def reread_partition_table(file: BinaryIO) -> None:
    """Force kernel to re-read the partition table on a block device.

    :param file: IO handle for the block device.
    """
    ioctl(file, BLKRRPART)
