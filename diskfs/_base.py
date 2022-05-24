"""Classes used across the modules of the ``disk`` package."""

from typing import NamedTuple

__all__ = [
    'ParseError',
    'AlignmentWarning',
    'BoundsError',
    'BoundsWarning',
    'SectorSize',
]


class ParseError(ValueError):
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
