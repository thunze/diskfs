"""Platform-specific disk operations for Linux systems."""

from __future__ import annotations

import sys

assert sys.platform == "linux"  # skipcq: BAN-B101

import os
from ctypes import c_uint
from fcntl import ioctl
from pathlib import Path
from typing import TYPE_CHECKING

from .base import DeviceProperties, SectorSize

if TYPE_CHECKING:
    from .typing import StrPath

__all__ = ["device_size", "device_sector_size", "reread_partition_table"]


BLKSSZGET = 0x1268
BLKPBSZGET = 0x127B
BLKRRPART = 0x125F


# noinspection PyUnusedLocal
def device_properties(fd: int, path: StrPath) -> DeviceProperties:
    """Return additional properties of a block device.

    :param fd: File descriptor for the block device.
    :param path: Path of the block device.
    """
    device = os.fstat(fd).st_rdev
    major, minor = os.major(device), os.minor(device)
    sysfs_base = Path(f"/sys/dev/block/{major}:{minor}")

    def read_text_or_none(path_: Path) -> str | None:
        """Return text of file at `path` and `rstrip()` the resulting text or
        return `None` if `path` is not a file.
        """
        try:
            return path_.read_text(encoding="utf-8").rstrip()
        except FileNotFoundError:
            return None

    removable_str = read_text_or_none(sysfs_base / "removable")
    vendor = read_text_or_none(sysfs_base / "device" / "vendor")
    model = read_text_or_none(sysfs_base / "device" / "model")

    removable = None
    if removable_str is not None:
        try:
            removable = bool(int(removable_str))
        except ValueError:  # pragma: no cover
            pass

    return DeviceProperties(removable, vendor, model)


def device_size(fd: int) -> int:
    """Return the size of a block device.

    :param fd: File descriptor for the block device.
    """
    return os.lseek(fd, 0, os.SEEK_END)


def device_sector_size(fd: int) -> SectorSize:
    """Return the logical and physical sector size of a block device.

    :param fd: File descriptor for the block device.
    """
    logical, physical = c_uint(), c_uint()  # see blkdev.h
    ioctl(fd, BLKSSZGET, logical)
    ioctl(fd, BLKPBSZGET, physical)
    return SectorSize(logical.value, physical.value)


def reread_partition_table(fd: int) -> None:
    """Update the operating system's view of a block device's partition table.

    :param fd: File descriptor for the block device.
    """
    ioctl(fd, BLKRRPART)
