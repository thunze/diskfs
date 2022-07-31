"""Classes and functions used across the ``fat`` package."""

from enum import Enum

from ..filesystem import FsType

__all__ = ['FatType']


class FatType(Enum):
    """FAT file system type."""

    FAT_12 = 12
    FAT_16 = 16
    FAT_32 = 32

    @classmethod
    def from_fs_type(cls, fs_type: FsType) -> 'FatType':
        return cls[fs_type.name]

    @property
    def fs_type(self) -> FsType:
        return FsType[self.name]
