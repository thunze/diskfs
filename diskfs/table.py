"""Protocols implemented in the ``table`` package and various partition entry checks."""

from __future__ import annotations

import warnings
from enum import Enum
from typing import TYPE_CHECKING, Any, Iterable, Protocol

from .base import AlignmentWarning, BoundsError, BoundsWarning, SectorSize

if TYPE_CHECKING:
    from .disk import Disk

__all__ = [
    "Table",
    "TableType",
    "PartitionEntry",
    "check_alignment",
    "check_bounds",
    "check_overlapping",
]


class TableType(Enum):
    """Partition table type."""

    MBR = 0
    GPT = 1


# noinspection PyPropertyDefinition
class PartitionEntry(Protocol):
    """Partition entry in a partition table."""

    @classmethod
    def new_empty(cls) -> PartitionEntry:
        """New empty / unused partition entry."""
        ...

    @classmethod
    def from_bytes(cls, b: bytes) -> PartitionEntry:
        """Parse partition entry from ``bytes``."""
        ...

    def __bytes__(self) -> bytes:
        """Get ``bytes`` representation of partition entry."""
        ...

    @property
    def start_lba(self) -> int:
        """Starting sector of the partition. Inclusive."""
        ...

    @property
    def end_lba(self) -> int:
        """Ending sector of the partition. Inclusive."""
        ...

    @property
    def length_lba(self) -> int:
        """Length of the partition in logical sectors."""
        ...

    @property
    def type(self) -> Any:
        """Partition type."""
        ...

    @property
    def empty(self) -> bool:
        """Whether the partition entry is considered empty / unused."""
        ...


# noinspection PyPropertyDefinition
class Table(Protocol):
    """Partition table."""

    @classmethod
    def from_disk(cls, disk: Disk) -> Table:
        """Parse partition table from ``disk``."""
        ...

    def _write_to_disk(self, disk: Disk) -> None:
        """Write partition table to ``disk``."""
        ...

    def usable_lba(self, disk_size: int, sector_size: SectorSize) -> tuple[int, int]:
        """Return a ``tuple`` of the first and last logical sector which may be used
        by a partition of this partition table.
        """
        ...

    @property
    def type(self) -> TableType:
        """Partition table type."""
        ...

    @property
    def partitions(self) -> tuple[PartitionEntry, ...]:
        ...


def check_overlapping(
    partitions: Iterable[PartitionEntry], *, warn: bool = False
) -> None:
    """Check if the partitions' bounds don't overlap with each other.

    By default, ``BoundsError`` is raised if any partitions are found to overlap with
    each other. If ``warn`` is ``True``, ``BoundsWarning`` is emitted instead of
    raising an exception.
    """
    partitions = tuple(partitions)
    # sort by starting sector
    partitions_sorted = sorted(partitions, key=lambda p: p.start_lba)
    prev_partition_end_lba = 0  # last sector of previous partition
    overlapping = False

    for partition in partitions_sorted:
        # Note: end_lba >= start_lba is already checked within the respective
        # PartitionEntry class.
        if partition.start_lba <= prev_partition_end_lba:
            overlapping = True
            break
        prev_partition_end_lba = partition.end_lba

    if overlapping:
        message = "At least one partition overlaps another partition"
        if warn:
            warnings.warn(message, BoundsWarning)
        else:
            raise BoundsError(message)


def check_bounds(
    partition: PartitionEntry, min_lba: int, max_lba: int, *, warn: bool = False
) -> None:
    """Check if a partition's bounds fall within the range of ``(min_lba, max_lba)``.

    Both ``min_lba`` and ``max_lba`` are *inclusive*.

    By default, ``BoundsError`` is raised if the partition doesn't fall within the.
    range of ``(min_lba, max_lba)``. If ``warn`` is ``True``, ``BoundsWarning`` is
    emitted instead of raising an exception.
    """
    start = partition.start_lba
    end = partition.end_lba

    if start < min_lba or end > max_lba:
        message = (
            f"Partition with bounds (LBA {start}, LBA {end}) does not fall within "
            f"the allowed range of (LBA {min_lba}, LBA {max_lba})"
        )
        if warn:
            warnings.warn(message, BoundsWarning)
        else:
            raise BoundsError(message)


def check_alignment(
    partition: PartitionEntry, sector_size: SectorSize, *, warn: bool = False
) -> bool:
    """Check if a partition's bounds align to the physical sector size of a disk.

    Returns ``True`` or ``False`` depending on whether the partition is properly
    aligned.

    If ``warn`` is `True``, ``AlignmentWarning`` is emitted if the partition is not
    properly aligned.
    """
    lss, pss = sector_size
    start_byte = partition.start_lba * lss
    end_byte = (partition.end_lba + 1) * lss  # exclusive

    if start_byte % pss != 0 or end_byte % pss != 0:
        if warn:
            warnings.warn(
                f"Partition with bounds ({start_byte}, {end_byte}) is not aligned to "
                f"physical sector size of {pss} bytes. This might lead to poor "
                f"I/O performance.",
                AlignmentWarning,
            )
        return False
    return True
