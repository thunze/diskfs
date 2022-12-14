"""Platform-specific disk operations for Linux systems."""

import sys

assert sys.platform == 'linux'  # skipcq: BAN-B101

import io
from fcntl import ioctl
from typing import BinaryIO

from .base import SectorSize

__all__ = ['device_size', 'device_sector_size', 'reread_partition_table']


BLKSSZGET = 0x1268
BLKPBSZGET = 0x127B
BLKRRPART = 0x125F


def device_size(file: BinaryIO) -> int:
    """Return the size of a block device.

    :param file: IO handle for the block device.
    """
    return file.seek(0, io.SEEK_END)


def device_sector_size(file: BinaryIO) -> SectorSize:
    """Get logical and physical sector size for a block device.

    :param file: IO handle for the block device.
    """
    logical = ioctl(file, BLKSSZGET)
    physical = ioctl(file, BLKPBSZGET)
    return SectorSize(logical, physical)


def reread_partition_table(file: BinaryIO) -> None:
    """Force kernel to re-read the partition table on a block device.

    :param file: IO handle for the block device.
    """
    ioctl(file, BLKRRPART)
