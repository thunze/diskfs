"""Platform-specific disk operations for Darwin systems."""

from __future__ import annotations

import sys

assert sys.platform == 'darwin'  # skipcq: BAN-B101

from ctypes import c_uint32, c_uint64
from fcntl import ioctl
from typing import BinaryIO

from .base import SectorSize

__all__ = ['device_size', 'device_sector_size', 'reread_partition_table']


DKIOCGETBLOCKSIZE = 0x40046418
DKIOCGETPHYSICALBLOCKSIZE = 0x4004644D
DKIOCGETBLOCKCOUNT = 0x40086419


def device_size(file: BinaryIO) -> int:
    """Return the size of a block device.

    :param file: IO handle for the block device.
    """
    sector_size, sector_count = c_uint32(), c_uint64()  # see disk.h
    ioctl(file, DKIOCGETBLOCKSIZE, sector_size)
    ioctl(file, DKIOCGETBLOCKCOUNT, sector_count)
    return sector_size.value * sector_count.value


def device_sector_size(file: BinaryIO) -> SectorSize:
    """Return the logical and physical sector size of a block device.

    :param file: IO handle for the block device.
    """
    logical, physical = c_uint32(), c_uint32()  # see disk.h
    ioctl(file, DKIOCGETBLOCKSIZE, logical)
    ioctl(file, DKIOCGETPHYSICALBLOCKSIZE, physical)
    return SectorSize(logical.value, physical.value)


# skipcq: PYL-W0613
# noinspection PyUnusedLocal
def reread_partition_table(file: BinaryIO) -> None:
    """Force kernel to re-read the partition table on a block device."""
