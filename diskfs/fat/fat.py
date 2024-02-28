"""File allocation table."""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from ..base import ValidationError
from ..filesystem import FileSystemLimit
from .base import FatType
from .reserved import BootSector

if TYPE_CHECKING:
    from ..volume import Volume

__all__ = ["Fat"]


CLUSTER_EMPTY = 0
CLUSTER_RESERVED = 1
BAD_CLUSTER = {
    FatType.FAT_12: 0xFF7,
    FatType.FAT_16: 0xFFF7,
    FatType.FAT_32: 0x0FFFFFF7,
}
CLUSTER_EOC = {
    FatType.FAT_12: 0xFFF,
    FatType.FAT_16: 0xFFFF,
    FatType.FAT_32: 0x0FFFFFFF,
}
CLUSTER_AVOID_DATA = {
    FatType.FAT_12: 0xFF0,
    FatType.FAT_16: 0xFFF0,
    FatType.FAT_32: 0x0FFFFFF0,
}


class Fat:
    """FAT region management.

    Choose a different FAT via ``main_fat`` if the first one is (partially)
    unreadable because of bad sectors.
    """

    def __init__(self, volume: Volume, boot_sector: BootSector, main_fat: int = 0):
        fat_size = boot_sector.fat_size
        fat_count = boot_sector.fat_region_size // boot_sector.fat_size
        fat_type = boot_sector.fat_type

        # Start sectors of available FATs
        fat_starts = tuple(
            boot_sector.fat_region_start + i * fat_size for i in range(fat_count)
        )

        # Data region starts with cluster 2
        expected_fat_clusters = boot_sector.total_clusters + 2
        read_max = BAD_CLUSTER[fat_type]

        # Reading from clusters ...FF6 and ...FF7 should be allowed if they exist
        if expected_fat_clusters > read_max + 1:
            raise ValueError(
                f"Total cluster number {boot_sector.total_clusters} is greater than "
                f"possible for FAT type {fat_type!s}"
            )

        # Round up for FAT12
        expected_fat_size_bytes = (expected_fat_clusters * fat_type.value - 1) // 8 + 1
        actual_fat_size_bytes = fat_size * volume.sector_size.logical

        if actual_fat_size_bytes < expected_fat_size_bytes:
            raise ValueError(
                f"FAT is too small for total cluster number (expected at least "
                f"{expected_fat_size_bytes} bytes, got {actual_fat_size_bytes} bytes)"
            )

        if not 0 <= main_fat < fat_count:
            raise ValueError(f"Main FAT number must be in range (0, {fat_count - 1})")

        self._volume = volume
        self._entries = expected_fat_clusters  # entries per FAT
        self._main_fat = main_fat
        self._fat_count = fat_count
        self._fat_size = fat_size
        self._fat_starts = fat_starts
        self._fat_type = fat_type

        # Values: Actual buffer, FAT sector offset, whether the buffer was altered
        # Inserting dummy values here which are immediately replaced by _ensure_buffer()
        self._buffer: tuple[bytearray, int, bool] = (bytearray(), -1, False)
        self._ensure_buffer(0)

        # Check media descriptor entry
        expected_media_type = boot_sector.bpb.bpb_dos_200.media_type
        actual_media_type = self[0] & 0xFF
        if actual_media_type != expected_media_type:
            raise ValidationError(
                "Media descriptor in FAT does not match media descriptor in BPB"
            )

    def _check_cluster_key(self, cluster: int) -> None:
        """Raise ``IndexError`` if ``cluster`` not a valid cluster index for the FAT."""
        key_max = self._entries - 1
        if not 0 <= cluster <= key_max:
            raise IndexError(f"Cluster index must not exceed FAT bounds (0, {key_max})")

    def _check_cluster_value(self, cluster: int) -> None:
        """Raise ``ValueError`` if ``cluster`` not a valid cluster value for the FAT."""
        value_max = CLUSTER_EOC[self._fat_type]
        if not 0 <= cluster <= value_max:
            raise ValueError(f"Cluster value must be in range (0, {value_max})")

    def _check_cluster_data_read(self, cluster: int) -> None:
        """Raise ``ValueError`` if ``cluster`` is not the number of a readable cluster
        with respect to the FAT.
        """
        read_max = self._entries - 1
        if not 2 <= cluster <= read_max:
            raise ValueError(
                f"Cluster number for read operation must be in range (2, {read_max})"
            )

    def _check_cluster_data_write(self, cluster: int) -> None:
        """Raise ``ValueError`` if ``cluster`` is not the number of a writable cluster
        with respect to the FAT.
        """
        write_max = min(self._entries, CLUSTER_AVOID_DATA[self._fat_type]) - 1
        if not 2 <= cluster <= write_max:
            raise ValueError(
                f"Cluster number for write operation must be in range (2, {write_max})"
            )

    def __len__(self) -> int:
        """Number of FAT entries."""
        return self._entries

    def _ensure_buffer(self, sector_offset: int) -> None:
        """Ensure that the internal buffer holds the data (usually one sector)
        found at the start sector of the selected main FAT + ``sector_offset``.

        If a data range other than the one already held by the buffer is requested,
        the buffer is flushed before data is read from the disk.

        For FAT12, two sectors are read instead of one. This is done so that we don't
        have to worry about FAT12 entries spanning a sector boundary.
        """
        if sector_offset >= self._fat_size:
            raise ValueError(f"Offset {sector_offset} exceeds FAT size")

        if self._buffer[1] == sector_offset:
            return  # Same part of the FAT, do nothing
        else:
            self.flush()  # Write to volume if there were changes

        start_sector = self._fat_starts[self._main_fat]
        if self._fat_type is FatType.FAT_12:
            sectors = 2
        else:
            sectors = 1

        b = self._volume.read_at(start_sector + sector_offset, sectors)
        self._buffer = (bytearray(b), sector_offset, False)

    def flush(self) -> None:
        """Write internal buffer to all FATs if it was altered."""
        buffer, sector_offset, altered = self._buffer

        # Only write buffer to volume if it was altered.
        if altered:
            for fat_start in self._fat_starts:
                self._volume.write_at(fat_start + sector_offset, bytes(buffer))
            self._buffer = (buffer, sector_offset, False)

    def _get_io_info(self, key: int) -> tuple[int, int, int]:
        """Return a ``tuple`` of ``(sector_offset, bytes_offset_sector, byte_count)``
        to be used for read and write operations on the FAT table.
        """
        if self._fat_type is FatType.FAT_12:
            bytes_offset = key + key // 2
            byte_count = 2
        elif self._fat_type is FatType.FAT_16:
            bytes_offset = key * 2
            byte_count = 2
        else:
            bytes_offset = key * 4
            byte_count = 4

        sector_offset = bytes_offset // self._volume.sector_size.logical
        bytes_offset_sector = bytes_offset % self._volume.sector_size.logical
        return sector_offset, bytes_offset_sector, byte_count

    def __getitem__(self, key: int) -> int:
        """Read the FAT entry with index ``key``."""
        self._check_cluster_key(key)
        sector_offset, bytes_offset_buffer, byte_count = self._get_io_info(key)

        self._ensure_buffer(sector_offset)
        buffer, _, _ = self._buffer

        value_bytes = buffer[bytes_offset_buffer : bytes_offset_buffer + byte_count]
        value = int.from_bytes(value_bytes, "little")

        if self._fat_type is FatType.FAT_12:
            if key & 1:
                return value >> 4
            else:
                return value & 0x0FFF

        if self._fat_type is FatType.FAT_32:
            return value & 0x0FFFFFFF

        return value

    def __setitem__(self, key: int, value: int) -> None:
        """Set value of the FAT entry with index ``key``."""
        self._volume.check_writable()

        self._check_cluster_key(key)
        self._check_cluster_value(value)
        sector_offset, bytes_offset_sector, byte_count = self._get_io_info(key)

        self._ensure_buffer(sector_offset)
        buffer, _, _ = self._buffer

        if self._fat_type in (FatType.FAT_12, FatType.FAT_32):
            old_value_bytes = buffer[
                bytes_offset_sector : bytes_offset_sector + byte_count
            ]
            old_value = int.from_bytes(old_value_bytes, "little")

            if self._fat_type is FatType.FAT_12:
                if key & 1:
                    value = (old_value & 0x000F) | (value << 4)
                else:
                    value = (old_value & 0xF000) | (value & 0x0FFF)

            elif self._fat_type is FatType.FAT_32:
                # High 4 bits are reserved, we must keep them
                value = (old_value & 0xF0000000) | value

        value_bytes = value.to_bytes(byte_count, "little")
        buffer[bytes_offset_sector : bytes_offset_sector + byte_count] = value_bytes
        self._buffer = (buffer, sector_offset, True)  # We altered the buffer

    def set_eoc(self, key: int) -> None:
        """Mark FAT entry with index ``key`` as the end of a cluster chain."""
        eoc = CLUSTER_EOC[self._fat_type]
        self[key] = eoc

    def set_empty(self, key: int) -> None:
        """Mark FAT entry with index ``key`` as unused."""
        self[key] = CLUSTER_EMPTY

    def get_chain(self, start_cluster: int) -> Iterator[int]:
        """Yield cluster numbers of cluster chain starting with ``start_cluster``."""
        bad_cluster = BAD_CLUSTER[self._fat_type]
        cluster = start_cluster

        while CLUSTER_RESERVED < cluster <= bad_cluster:
            # Even bad clusters should be tried to read from if the FAT is long enough
            # to support that.
            self._check_cluster_data_read(cluster)
            yield cluster
            cluster = self[cluster]

    def next_free_clusters(self, count: int) -> Iterator[int]:
        """Yield the numbers of the next ``count`` free clusters."""
        avoid_data = CLUSTER_AVOID_DATA[self._fat_type]
        found = 0  # Count of free clusters already found
        # noinspection PyTypeChecker
        for key, value in enumerate(iter(self)):
            if found == count:
                return
            if key >= avoid_data:
                break
            if value == CLUSTER_EMPTY:
                yield key
                found += 1
        raise FileSystemLimit("Not enough free clusters available")

    def free_clusters(self) -> int:
        """Return the total count of free clusters.

        Currently very inefficient, even for FAT32 because the FS info sector is not
        utilized.
        """
        # noinspection PyTypeChecker
        return sum(1 for value in iter(self) if value == CLUSTER_EMPTY)

    @property
    def main_fat(self) -> int:
        """The selected main FAT."""
        return self._main_fat
