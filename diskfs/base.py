"""Exceptions and helper functions used across ``diskfs``."""

from typing import NamedTuple

__all__ = [
    'ValidationError',
    'ValidationWarning',
    'BoundsError',
    'BoundsWarning',
    'AlignmentWarning',
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
