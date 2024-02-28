"""Disk access.

A disk represents either a file or block device that one can access and manipulate.
"""

from __future__ import annotations

import logging
import os
import sys
from stat import S_ISBLK, S_ISREG
from types import TracebackType
from typing import TYPE_CHECKING, Any

from . import gpt, mbr
from .base import SectorSize, ValidationError
from .table import Table
from .volume import Volume

if sys.platform == "win32":
    from .win32 import device_sector_size, device_size, reread_partition_table
elif sys.platform == "linux":
    from .linux import device_sector_size, device_size, reread_partition_table
elif sys.platform == "darwin":
    from .darwin import device_sector_size, device_size, reread_partition_table
else:
    raise RuntimeError(f"Unspported platform {sys.platform!r}")

if TYPE_CHECKING:
    from .typing import ReadableBuffer, StrPath

__all__ = ["Disk"]


log = logging.getLogger(__name__)


if hasattr(os, "pread") and hasattr(os, "pwrite"):
    _read = os.pread
    _write = os.pwrite
else:

    def _read(fd: int, size: int, pos: int) -> bytes:
        """Read `size` bytes from file descriptor `fd` starting at byte `pos`."""
        os.lseek(fd, pos, os.SEEK_SET)
        return os.read(fd, size)

    def _write(fd: int, b: ReadableBuffer, pos: int) -> int:
        """Write raw bytes `b` to file descriptor `fd` starting at byte `pos`."""
        os.lseek(fd, pos, os.SEEK_SET)
        return os.write(fd, b)


class Disk:
    """File or block device that one can access and manipulate.

    Also serves as an accessor to the underlying file or block device.

    Do not use `__init__` directly, use `Disk.open()` or `Disk.new()` instead.
    """

    def __init__(
        self,
        fd: int,
        path: StrPath,
        size: int,
        sector_size: SectorSize,
        device: bool,
        writable: bool,
    ):
        self._fd = fd
        self._path = str(path)
        self._size = size
        self._sector_size = sector_size
        self._device = device
        self._writable = writable

        self._closed = False
        self._table: Table | None = None

        log.info(f"Opened disk {self}")
        log.info(f"{self} - Size: {size} bytes, {sector_size}")
        self.read_table()

    @classmethod
    def new(cls, path: StrPath, size: int, *, sector_size: int = 512) -> Disk:
        """Create a new disk image at `path`."""
        if size <= 0:
            raise ValueError("Disk size must be greater than 0")
        if sector_size <= 0:
            raise ValueError("Sector size must be greater than 0")

        flags = os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_BINARY", 0)
        fd = os.open(path, flags, 0o666)
        try:
            os.truncate(fd, size)
            simulated_sector_size = SectorSize(sector_size, sector_size)
            return cls(fd, path, size, simulated_sector_size, False, True)
        except BaseException:
            os.close(fd)
            raise

    @classmethod
    def open(
        cls, path: StrPath, *, sector_size: int = None, readonly: bool = True
    ) -> Disk:
        """Open block device or disk image at `path`."""
        read_write_flag = os.O_RDONLY if readonly else os.O_RDWR
        flags = read_write_flag | getattr(os, "O_BINARY", 0)
        fd = os.open(path, flags)

        try:
            stat = os.fstat(fd)
            block_device = S_ISBLK(stat.st_mode)
            regular_file = S_ISREG(stat.st_mode)

            if block_device:
                if sector_size is not None:
                    raise ValueError("Sector size cannot be set for block devices")

                size = device_size(fd)
                real_sector_size = device_sector_size(fd)
                return cls(fd, path, size, real_sector_size, True, not readonly)

            if regular_file:
                if sector_size is None:
                    raise ValueError("Sector size must be set for regular file")
                if sector_size <= 0:
                    raise ValueError("Sector size must be greater than 0")

                size = stat.st_size
                simulated_sector_size = SectorSize(sector_size, sector_size)
                return cls(fd, path, size, simulated_sector_size, False, not readonly)

            raise ValueError("File is neither a block device nor a regular file")

        except BaseException:
            os.close(fd)
            raise

    def read_at(self, pos: int, size: int) -> bytes:
        """Read `size` sectors from the disk starting at sector `pos`.

        Uses the logical sector size of the disk.
        """
        self.check_closed()

        if pos < 0:
            raise ValueError("Position to read from must be zero or positive")
        if size < 0:
            raise ValueError("Amount of sectors to read must be zero or positive")
        if size == 0:
            return b""

        pos_bytes = pos * self.sector_size.logical
        size_bytes = size * self.sector_size.logical

        if pos_bytes + size_bytes > self._size:
            raise ValueError("Sector range out of disk bounds")

        b = _read(self._fd, size_bytes, pos_bytes)

        if len(b) != size_bytes:
            raise ValueError(
                f"Did not read the expected amount of bytes (expected {size} bytes, "
                f"got {len(b)} bytes)"
            )
        return b

    def write_at(
        self, pos: int, b: ReadableBuffer, *, fill_zeroes: bool = False
    ) -> None:
        """Write raw bytes `b` to the disk starting at sector `pos`.

        Uses the logical sector size of the disk.

        :param pos: LBA to write at.
        :param b: Bytes to write.
        :param fill_zeroes: Whether to fill up the last sector to write at with zeroes
            if b doesn't cover the whole sector.
        """
        self.check_closed()
        self.check_writable()

        if pos < 0:
            raise ValueError("Position to write at must be zero or positive")
        if not isinstance(b, memoryview):
            b = memoryview(b).cast("B")
        size = b.nbytes
        if size == 0:
            return

        lss = self._sector_size.logical
        remainder = size % lss

        if remainder != 0:
            if not fill_zeroes:
                raise ValueError(
                    f"Can only write in multiples of {lss} bytes (logical sector size)"
                )
            zeroes = b"\x00" * (lss - remainder)
            b = bytes(b) + zeroes
            size = len(b)

        pos_bytes = pos * self.sector_size.logical
        if pos_bytes + size > self._size:
            raise ValueError("Sector range out of disk bounds")

        bytes_written = _write(self._fd, b, pos_bytes)

        if bytes_written != size:
            raise ValueError(
                f"Did not write the expected amount of bytes (expected {size} "
                f"bytes, wrote {bytes_written} bytes)"
            )

    def flush(self) -> None:
        """Flush write buffers of the underlying file or block device, if applicable."""
        self.check_closed()
        os.fsync(self._fd)

    def read_table(self) -> None:
        """Try to read a partition table on the disk and update the `Disk` object
        accordingly.

        If no partition table can be parsed, the disk is considered unpartitioned.
        """
        self.check_closed()
        try:
            self._table = gpt.Table.from_disk(self)
        except ValidationError:
            try:
                self._table = mbr.Table.from_disk(self)
            except ValidationError:
                # no valid partition table found
                self._table = None

        if self._table is None:
            log.info(f"{self} - No valid partition table found")
        else:
            log.info(f"{self} - Found partition table {self._table}")

    def clear(self) -> None:
        """Clear the disk by overwriting specific parts of the disk with zeroes.

        **Caution:** This will overwrite the disk's partition table and thus remove
        access to any partitions residing on the disk. If any file systems reside on
        the disk, they will very likely be destroyed as well. Always create a backup
        of your data before clearing a disk.
        """
        self.check_closed()
        self.check_writable()
        log.info(f"{self} - Clearing disk")

        self.flush()
        self._table = None

        if self._device:
            reread_partition_table(self._fd)
        raise NotImplementedError

    def partition(self, table: Table) -> None:
        """Apply a partition table to the disk.

        **Caution:** If a file system resides on the unpartitioned disk, it will very
        likely be overwritten and thus be rendered unusable. Always create a backup
        of your data before (re-)partitioning a disk.

        If the disk is already partitioned, `ValueError` will be raised.
        """
        self.check_closed()
        self.check_writable()

        if self._table is not None:
            raise ValueError(
                "Disk is already partitioned; clear disk first to re-partition"
            )
        log.info(f"{self} - Partitioning disk using partition table {table}")

        # skipcq: PYL-W0212
        # noinspection PyProtectedMember
        table._write_to_disk(self)
        self.flush()
        self._table = table

        if self._device:
            reread_partition_table(self._fd)

    def volume(self, partition: int = None) -> Volume:
        """Get the volume corresponding to partition `partition` on the disk.

        If `partition` is not specified and the disk is unpartitioned, a volume
        spanning the whole disk is returned.
        """
        self.check_closed()
        if partition is None:
            if self._table is not None:
                raise ValueError(
                    "Disk is partitioned; please specify a partition number"
                )
            disk_end = self._size // self.sector_size.logical - 1
            return Volume(self, 0, disk_end)

        if partition is not None:
            if self._table is None:
                raise ValueError(
                    "Disk is unpartitioned; you cannot specify a partition number"
                )
            if not 0 <= partition < len(self._table.partitions):
                raise IndexError("Partition number out of range")

            entry = self._table.partitions[partition]
            return Volume(self, entry.start_lba, entry.end_lba)

    def dismount_volumes(self) -> None:
        """Dismount all volumes associated with the disk."""
        self.check_closed()
        if not self._device:
            raise ValueError("Can only dismount volumes of block devices")
        log.info(f"{self} - Dismounting volumes")
        raise NotImplementedError

    def close(self) -> None:
        """Close the underlying IO object.

        This method has no effect if the IO object is already closed.
        """
        if self._closed:
            return
        os.close(self._fd)
        self._closed = True
        log.info(f"Closed disk {self}")

    def __enter__(self) -> Disk:
        """Context management protocol."""
        self.check_closed()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType,
    ) -> None:
        """Context management protocol."""
        self.close()

    @property
    def device(self) -> bool:
        """Whether the disk's data resides on a block device instead of a file."""
        return self._device

    @property
    def size(self) -> int:
        """Size of the disk in bytes."""
        return self._size

    @property
    def sector_size(self) -> SectorSize:
        """Logical and physical sector size of the disk, each in bytes."""
        return self._sector_size

    @property
    def table(self) -> Table | None:
        """Partition table last detected on the disk.

        `None` if no partition table was detected at that time.
        """
        return self._table

    @property
    def closed(self) -> bool:
        """Whether the underlying file or block device is closed."""
        return self._closed

    @property
    def writable(self) -> bool:
        """Whether the underlying file or block device supports writing."""
        self.check_closed()
        return self._writable

    def check_closed(self) -> None:
        """Raise `ValueError` if the underlying file or block device is closed."""
        if self._closed:
            raise ValueError("I/O operation on closed disk")

    def check_writable(self) -> None:
        """Raise `ValueError` if the underlying file or block device is read-only."""
        if not self._writable:
            raise ValueError("Disk is not writable")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Disk):
            return self._path == other._path
        return NotImplemented

    def __str__(self) -> str:
        return self._path

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._path}, size={self._size})"
