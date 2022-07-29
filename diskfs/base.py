"""Classes and functions used across the ``disk`` package."""

from typing import NamedTuple

__all__ = [
    'ValidationError',
    # 'ValidationWarning',
    'BoundsError',
    'BoundsWarning',
    'AlignmentWarning',
    'SectorSize',
    'is_power_of_two',
]


class ValidationError(ValueError):
    """Exception raised if a specific structure -- for example a partition table or a
    file system header -- could not be created because the passed data does not
    conform to the standard of the structure to parse.
    """


class BoundsError(ValueError):
    """Exception raised if a partition's or file system's bounds are deemed illegal."""


class BoundsWarning(UserWarning):
    """Warning emitted if a partition's or file system's bounds are deemed illegal."""


class AlignmentWarning(UserWarning):
    """Warning emitted if a partition or file system is found not to be aligned to a
    disk's physical sector size.

    This is usually bad because it might lead to poor performance.
    """


class SectorSize(NamedTuple):
    """Tuple of logical and physical sector sizes of a disk."""

    logical: int
    physical: int


def is_power_of_two(value: int) -> bool:
    """Check if ``value`` is a power of two.

    ``value`` must be an ``int`` greater than zero.

    Returns whether ``value`` can be expressed as 2 to the power of x, while x is an
    integer greater than or equal to zero.
    """
    if value <= 0:
        raise ValueError('Value must be greater than 0')
    return value & (value - 1) == 0
