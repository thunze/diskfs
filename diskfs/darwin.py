"""Platform-specific disk operations for Darwin systems."""

import sys

assert sys.platform == 'darwin'  # skipcq: BAN-B101

import io
from ctypes import c_uint32
from fcntl import ioctl
from typing import BinaryIO

from .base import SectorSize

__all__ = ['device_size', 'device_sector_size', 'reread_partition_table']


DKIOCGETBLOCKSIZE = 0x40046418
DKIOCGETPHYSICALBLOCKSIZE = 0x4004644D


def device_size(file: BinaryIO) -> int:
    """Return the size of a block device.

    :param file: IO handle for the block device.
    """
    return file.seek(0, io.SEEK_END)


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
