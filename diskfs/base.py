"""Exception classes, data structures and helper functions used across ``diskfs``."""

from __future__ import annotations

from typing import NamedTuple

__all__ = [
    'ValidationError',
    'ValidationWarning',
    'BoundsError',
    'BoundsWarning',
    'AlignmentWarning',
    'DeviceProperties',
    'SectorSize',
    'is_power_of_two',
]


class ValidationError(ValueError):
    """Exception raised if an object representing a specific structure -- for example
    a partition table or a file system header -- cannot be created because the data
    to be parsed as the structure does not conform to the standard of the structure.
    """


class ValidationWarning(UserWarning):
    """Warning emitted if a value found in a structure does not conform to the
    standard of the structure but might still be usable.
    """


class BoundsError(ValueError):
    """Exception raised if the bounds of a partition or file system are considered
    illegal.
    """


class BoundsWarning(UserWarning):
    """Warning emitted if the bounds of a partition or file system are considered
    illegal.
    """


class AlignmentWarning(UserWarning):
    """Warning emitted if a partition or file system is found not to be aligned to a
    disk's physical sector size.

    This usually leads to significantly worse I/O performance caused by redundant
    read and write operations on the disk.
    """


class DeviceProperties(NamedTuple):
    """Additional properties provided for block devices.

    - ``removable``: Whether the device is removable.
    - ``vendor``: Name of the vendor of the device (e.g. 'SanDisk').
    - ``model``: Model or product name of the device (e.g. 'Ultra Fit').

    A property is ``None`` if it could not be determined for the block device.
    """

    removable: bool | None
    vendor: str | None
    model: str | None


class SectorSize(NamedTuple):
    """``NamedTuple`` of the logical and physical sector size of a disk."""

    logical: int
    physical: int


def is_power_of_two(value: int) -> bool:
    """Check if ``value`` is a power of two.

    ``value`` must be an ``int`` greater than zero.

    Returns whether ``value`` can be expressed as 2 to the power of x, with x being
    an integer greater than or equal to zero.
    """
    if value <= 0:
        raise ValueError('Value must be greater than 0')
    return value & (value - 1) == 0
