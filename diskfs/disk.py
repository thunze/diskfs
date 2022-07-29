"""Disk manipulation.

A disk represents either a file or block device that one can access and manipulate.
"""

import logging
import os
import sys
from stat import S_ISBLK, S_ISREG
from types import TracebackType
from typing import Any, BinaryIO, Optional, Type

from .base import SectorSize, ValidationError
from .table import Table, gpt, mbr

if sys.platform == 'win32':
    from .win32 import device_sector_size, device_size, reread_partition_table
elif sys.platform == 'linux':
    from .linux import device_sector_size, device_size, reread_partition_table
elif sys.platform == 'darwin':
    from .darwin import device_sector_size, device_size, reread_partition_table
else:
    raise RuntimeError(f'Unspported platform {sys.platform!r}')


__all__ = ['Disk']


log = logging.getLogger(__name__)


SECTOR_SIZE_DEFAULT = 512


class Disk:
    """File or block device that one can access and manipulate.

    Also serves as an accessor to the underlying file or block device.

    Do not use ``__init__`` directly, use ``Disk.open()`` or ``Disk.new()`` instead.
    """

    def __init__(
        self, file: BinaryIO, device: bool, size: int, sector_size: SectorSize
    ):
        self._file = file
        self._device = device
        self._size = size
        self._sector_size = sector_size
        self._table: Optional[Table] = None

        log.info(f'Opened disk {self}')
        log.info(f'{self} - Size: {size} bytes, {sector_size}')
        self.read_table()

    @classmethod
    def new(
        cls, path: str, size: int, *, sector_size: int = SECTOR_SIZE_DEFAULT
    ) -> 'Disk':
        """Create a new disk image at ``path``."""
        if size <= 0:
            raise ValueError('Disk size must be greater than 0')
        if sector_size <= 0:
            raise ValueError('Sector size must be greater than 0')

        file = open(path, 'xb+')  # skipcq: PTC-W6004
        try:
            file.truncate(size)
            return cls(file, False, size, SectorSize(sector_size, sector_size))
        except BaseException:
            file.close()
            raise

    @classmethod
    def open(
        cls, path: str, *, sector_size: int = None, readonly: bool = False
    ) -> 'Disk':
        """Open block device or disk image at ``path``."""
        if readonly:
            file = open(path, 'rb')
        else:
            file = open(path, 'rb+')

        try:
            stat = os.stat(file.name)
            block_device = S_ISBLK(stat.st_mode)
            regular_file = S_ISREG(stat.st_mode)

            if block_device:
                if sector_size is not None:
                    raise ValueError('Sector size cannot be set for block devices')
                size = device_size(file)
                real_sector_size = device_sector_size(file)
                return cls(file, True, size, real_sector_size)

            if regular_file:
                if sector_size is None:
                    raise ValueError('Sector size must be set for regular file')
                if sector_size <= 0:
                    raise ValueError('Sector size must be greater than 0')
                size = stat.st_size
                return cls(file, False, size, SectorSize(sector_size, sector_size))

            raise ValueError('File is neither a block device nor a regular file')

        except BaseException:
            file.close()
            raise

    def read_at(self, pos: int, size: int) -> bytes:
        """Read ``size`` sectors from the disk starting at sector ``pos``.

        Uses the logical sector size of the disk.
        """
        self._check_closed()
        log.debug(f'{self} - Reading {size} sectors starting at sector {pos}')

        if pos < 0:
            raise ValueError('Position to read from must be zero or positive')
        if size < 0:
            raise ValueError('Amount of sectors to read must be zero or positive')
        if size == 0:
            return b''

        pos_bytes = pos * self.sector_size.logical
        size_bytes = size * self.sector_size.logical

        if pos_bytes + size_bytes > self._size:
            raise ValueError('Sector range out of disk bounds')

        try:
            self._file.seek(pos_bytes)
            b = self._file.read(size_bytes)
        except PermissionError:
            # Special case for block devices on Windows:
            # When trying to read sectors from a block device beyond its size,
            # PermissionError is raised. Thus, trying to read the last few sectors
            # of a block device might also raise PermissionError if buffered IO is
            # used (which is the case here). We solve this by using a separate,
            # unbuffered file object to read from the block device.
            if sys.platform == 'win32' and self._device:
                with open(self._file.name, 'rb', buffering=0) as unbuf_file:
                    unbuf_file.seek(pos_bytes)
                    b = unbuf_file.read(size_bytes)
            else:
                raise

        if len(b) != size_bytes:
            raise ValueError(
                f'Did not read the expected amount of bytes (expected {size} bytes, '
                f'got {len(b)} bytes)'
            )
        return b

    def write_at(self, pos: int, b: bytes, *, fill_zeroes: bool = False) -> None:
        """Write raw bytes ``b`` to the disk while starting at sector ``pos``.

        Uses the logical sector size of the disk.

        :param pos: LBA to write at.
        :param b: Bytes to write.
        :param fill_zeroes: Whether to fill up the last sector to write at with zeroes
            if b doesn't cover the whole sector.
        """
        self._check_closed()
        self._check_writable()
        log.debug(f'{self} - Writing {len(b)} bytes starting at sector {pos}')

        pos_bytes = pos * self.sector_size.logical

        if pos < 0:
            raise ValueError('Position to write at must be zero or positive')
        if pos_bytes + len(b) > self._size:
            raise ValueError('Sector range out of bounds')

        lss = self._sector_size.logical
        remainder = len(b) % lss

        if remainder != 0:
            if not fill_zeroes:
                raise ValueError(
                    f'Can only write in multiples of {lss} bytes (logical sector size)'
                )
            zeroes = b'\x00' * (lss - remainder)
            b += zeroes

        self._file.seek(pos_bytes)
        bytes_written = self._file.write(b)
        if len(b) != bytes_written:
            raise ValueError(
                f'Did not write the expected amount of bytes (expected {len(b)} '
                f'bytes, wrote {bytes_written} bytes)'
            )

    def flush(self) -> None:
        """Flush write buffers of the underlying file or block device, if applicable."""
        self._check_closed()
        self._file.flush()

    def read_table(self) -> None:
        """Try to read a partition table on the disk and update the ``Disk`` object
        accordingly.

        If no partition table can be parsed, the disk is considered unpartitioned.
        """
        self._check_closed()
        try:
            self._table = gpt.Table.from_disk(self)
        except ValidationError:
            try:
                self._table = mbr.Table.from_disk(self)
            except ValidationError:
                # no valid partition table found
                self._table = None

        if self._table is None:
            log.info(f'{self} - No valid partition table found')
        else:
            log.info(f'{self} - Found partition table {self._table}')

    def clear(self) -> None:
        """Clear the disk by overwriting specific parts of the disk with zeroes.

        **Caution:** This will overwrite the disk's partition table and thus remove
        access to any partitions residing on the disk. If any file systems reside on
        the disk, they will very likely be destroyed as well. Always create a backup
        of your data before clearing a disk.
        """
        self._check_closed()
        self._check_writable()
        log.info(f'{self} - Clearing disk')
        self.flush()
        raise NotImplementedError

    def partition(self, table: Table) -> None:
        """Apply a partition table to the disk.

        **Caution:** If a file system resides on the unpartitioned disk, it will very
        likely be overwritten and thus be rendered unusable. Always create a backup
        of your data before (re-)partitioning a disk.

        If the disk is already partitioned, ``ValueError`` will be raised.
        """
        self._check_closed()
        self._check_writable()

        if self._table is not None:
            raise ValueError(
                'Disk is already partitioned; clear disk first to re-partition'
            )
        log.info(f'{self} - Partitioning disk using partition table {table}')

        # skipcq: PYL-W0212
        # noinspection PyProtectedMember
        table._write_to_disk(self)
        self.flush()
        self._table = table

        if self._device:
            reread_partition_table(self._file)

    def get_filesystem(self, partition: int = None) -> None:
        """Get a specific file system residing on the disk.

        If ``partition`` is not specified, it is tried to parse a standalone file
        system from the unpartitioned disk.
        """
        self._check_closed()
        if partition is None and self._table is not None:
            raise ValueError('Disk is partitioned; please specify a partition number')
        if partition is not None and self._table is None:
            raise ValueError(
                'Disk is unpartitioned; you cannot specify a partition number'
            )
        raise NotImplementedError

    def dismount_volumes(self) -> None:
        """Dismount all volumes associated with the disk."""
        self._check_closed()
        if not self._device:
            raise ValueError('Can only dismount volumes of block devices')
        log.info(f'{self} - Dismounting volumes')
        raise NotImplementedError

    def close(self) -> None:
        """Close the underlying IO object.

        This method has no effect if the IO object is already closed.
        """
        self._file.close()
        log.info(f'Closed disk {self}')

    def __enter__(self) -> 'Disk':
        """Context management protocol."""
        self._check_closed()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
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
    def table(self) -> Optional[Table]:
        """Partition table last detected on the disk.

        ``None`` if no partition table was detected at that time.
        """
        return self._table

    @property
    def closed(self) -> bool:
        """Whether the underlying file or block device is closed."""
        return self._file.closed

    @property
    def writable(self) -> bool:
        """Whether the underlying file or block device supports writing."""
        return self._file.writable()

    def _check_closed(self) -> None:
        """Raise ``ValueError`` if the underlying file or block device is closed."""
        if self.closed:
            raise ValueError('I/O operation on closed disk')

    def _check_writable(self) -> None:
        """Raise ``ValueError`` if the underlying file or block device is read-only."""
        if not self.writable:
            raise ValueError('Disk is not writable')

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Disk):
            return self._file.name == other._file.name
        return NotImplemented

    def __str__(self) -> str:
        return self._file.name

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self._file.name}, size={self._size})'
