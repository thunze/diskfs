"""Tests for the `base` module of the `fat` package."""

from __future__ import annotations

import pytest

from diskfs.fat.base import FatType
from diskfs.filesystem import FsType


@pytest.mark.parametrize(
    ["fs_type", "fat_type"],
    [
        (FsType.FAT_12, FatType.FAT_12),
        (FsType.FAT_16, FatType.FAT_16),
        (FsType.FAT_32, FatType.FAT_32),
    ],
)
def test_fat_type_mapping(fs_type, fat_type):
    """Test mapping between `FatType` and `FsType`."""
    assert FatType.from_fs_type(fs_type) is fat_type
    assert fat_type.fs_type is fs_type
