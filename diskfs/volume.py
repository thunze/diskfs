"""Volume access.

A volume represents a contiguous part of a disk and may hold a file system.
"""

from typing import TYPE_CHECKING, Any

from .base import SectorSize

if TYPE_CHECKING:
    from .disk import Disk
    from .typing_ import ReadableBuffer

__all__ = ['Volume']


class Volume:
    """Contiguous part of a disk which may hold a file system.

    The bounds of a volume usually correspond to either the bounds of a partition on
    the disk or the bounds of the underlying disk itself, meaning that the volume
    spans the whole disk.

    If a volume holds a file system, the file system might only span part of the
    volume, but it must start at sector 0 of the volume. So unlike on Windows
    operating systems, the size of a volume usually doesn't correspond to the amount
    of disk space the file system it holds provides.

    Also serves as an accessor to the underlying disk.
    """

    def __init__(self, disk: 'Disk', start_lba: int, end_lba: int):
        disk_end_lba = disk.size // disk.sector_size.logical - 1

        if not 0 <= start_lba <= disk_end_lba:
            raise ValueError(
                f'Volume start sector must be in range (0, {disk_end_lba}), got '
                f'{start_lba}'
            )
        if not start_lba <= end_lba <= disk_end_lba:
            raise ValueError(
                f'Volume end sector must be in range ({start_lba}, {disk_end_lba}), '
                f'got {end_lba}'
            )

        disk.check_closed()
        self._disk = disk
        self._start_lba = start_lba
        self._end_lba = end_lba

    def read_at(self, pos: int, size: int) -> bytes:
        if not 0 <= pos < self.size_lba:
            raise ValueError('Position to read from out of volume bounds')
        if not 0 <= size <= self.size_lba - pos:
            raise ValueError('Sector range out of volume bounds')

        disk_pos = self._start_lba + pos
        return self._disk.read_at(disk_pos, size)

    def write_at(
        self, pos: int, b: 'ReadableBuffer', *, fill_zeroes: bool = False
    ) -> None:
        if not 0 <= pos < self.size_lba:
            raise ValueError('Position to write at out of volume bounds')
        with memoryview(b) as view:
            size = view.nbytes

        # Disk only accepts writing in multiples of lss anyway, so we can round up.
        sectors_to_write = (size - 1) // self._disk.sector_size.logical + 1
        if not 0 <= sectors_to_write <= self.size_lba - pos:
            raise ValueError('Sector range out of volume bounds')

        disk_pos = self._start_lba + pos
        return self._disk.write_at(disk_pos, b, fill_zeroes=fill_zeroes)

    def flush(self) -> None:
        self._disk.flush()

    def clear(self) -> None:
        """Clear the volume by overwriting specific parts with zeroes.

        **Caution:** This will destroy any file system residing on the volume. Always
        create a backup of your data before clearing a volume.
        """
        self._disk.check_closed()
        self._disk.check_writable()
        self._disk.flush()
        raise NotImplementedError

    @property
    def disk(self) -> 'Disk':
        return self._disk

    @property
    def start_lba(self) -> int:
        return self._start_lba

    @property
    def end_lba(self) -> int:
        return self._end_lba

    @property
    def size_lba(self) -> int:
        return self._end_lba - self._start_lba + 1

    @property
    def sector_size(self) -> SectorSize:
        return self._disk.sector_size

    @property
    def closed(self) -> bool:
        return self._disk.closed

    @property
    def writable(self) -> bool:
        return self._disk.writable

    def check_closed(self) -> None:
        return self._disk.check_closed()

    def check_writable(self) -> None:
        return self._disk.check_writable()

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Volume):
            return (
                self._disk == other._disk
                and self._start_lba == other._start_lba
                and self._end_lba == other._end_lba
            )
        return NotImplemented

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}({self._disk}, start_lba={self._start_lba}, '
            f'size_lba={self.size_lba})'
        )
