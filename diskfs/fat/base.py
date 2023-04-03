"""Classes used across the ``fat`` package."""

from __future__ import annotations

from enum import Enum

from ..filesystem import FsType

__all__ = ['FatType']


class FatType(Enum):
    """FAT file system type."""

    FAT_12 = 12
    FAT_16 = 16
    FAT_32 = 32

    @classmethod
    def from_fs_type(cls, fs_type: FsType) -> FatType:
        """Return the ``FatType`` corresponding to ``fs_type``.

        If ``fs_type`` represents a non-FAT file system type, ``KeyError`` is raised.
        """
        return cls[fs_type.name]

    @property
    def fs_type(self) -> FsType:
        """``FsType`` corresponding to the ``FatType``."""
        return FsType[self.name]
