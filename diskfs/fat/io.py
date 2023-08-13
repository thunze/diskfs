"""IO classes."""

from __future__ import annotations

from datetime import datetime
from io import SEEK_CUR, SEEK_END, SEEK_SET, RawIOBase
from typing import TYPE_CHECKING

from ..filesystem import FileSystemLimit, FsType
from .directory import Attributes, Entry

if TYPE_CHECKING:
    from ..typing import ReadableBuffer, WriteableBuffer
    from ..volume import Volume
    from .filesystem import FileSystem

__all__ = ['DataIO', 'RootdirIO']


class _InternalIO(RawIOBase):
    """Base class for internally used file-like objects."""

    _pos: int
    _size: int
    _unit_size: int
    _volume: Volume

    def _check_closed(self) -> None:
        self._volume.check_closed()
        if self.closed:
            raise ValueError('I/O operation on closed file')

    def _check_writable(self) -> None:
        self._volume.check_writable()

    def _allocate(self, min_size: int) -> int:
        raise NotImplementedError

    def _free(self, max_size: int) -> int:
        raise NotImplementedError

    def _read_units(self, pos: int, count: int) -> bytes:
        raise NotImplementedError

    def _write_units(self, pos: int, b: bytes | memoryview) -> None:
        raise NotImplementedError

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        self._check_closed()

        if whence == SEEK_SET:
            if offset < 0:
                raise ValueError(f'Negative seek position {offset}')
            self._pos = offset
        elif whence == SEEK_CUR:
            self._pos = max(0, self._pos + offset)
        elif whence == SEEK_END:
            self._pos = max(0, self._size + offset)
        else:
            raise ValueError('Unsupported whence value, must be one of (0, 1, 2)')
        return self._pos

    def read(self, size: int = -1) -> bytes:
        self._check_closed()

        if size == 0 or self._pos >= self._size:
            return b''
        if size < 0 or self._pos + size > self._size:
            size = self._size - self._pos

        stop = self._pos + size
        start_unit = self._pos // self._unit_size
        stop_unit = (stop - 1) // self._unit_size + 1
        units = stop_unit - start_unit

        b_start = self._pos % self._unit_size
        b = self._read_units(start_unit, units)

        self._pos += size
        return b[b_start : b_start + size]

    def readinto(self, b: WriteableBuffer) -> int:
        m = memoryview(b).cast('B')
        data = self.read(len(m))
        n = len(data)
        m[:n] = data
        return n

    def write(self, b: ReadableBuffer) -> int:
        self._check_closed()
        self._check_writable()

        if not isinstance(b, memoryview):
            b = memoryview(b)
        size = b.nbytes
        if size == 0:
            return 0
        b = b.cast('B')

        stop = self._pos + size
        self._allocate(stop)

        start_unit = self._pos // self._unit_size
        stop_unit = (stop - 1) // self._unit_size + 1
        units = stop_unit - start_unit
        b_start = self._pos % self._unit_size
        to_write: bytes | memoryview

        if b_start == 0 and size % self._unit_size == 0:
            # Byte range is aligned, we can write without reading first.
            to_write = b
        elif units == 1:
            # Byte range is contained by one unit.
            unit_bytes = self._read_units(start_unit, 1)
            to_write = unit_bytes[:b_start] + bytes(b) + unit_bytes[b_start + size :]
        else:
            # Read first and last unit, insert b.
            first_unit = self._read_units(start_unit, 1)
            last_unit = self._read_units(stop_unit - 1, 1)
            keep_last_unit = (b_start + size) % self._unit_size
            to_write = first_unit[:b_start] + bytes(b) + last_unit[keep_last_unit:]

        self._write_units(start_unit, to_write)
        self._pos += size
        return size

    def truncate(self, size: int = None) -> int:
        self._check_closed()
        self._check_writable()
        if size is None:
            size = self._pos
        if size < 0:
            raise ValueError(f'Invalid size {size}')

        if size > self._size:
            self._allocate(size)
        elif size < self._size:
            self._free(size)
        return size

    def flush(self) -> None:
        self._check_closed()
        self._volume.flush()

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return self._volume.writable

    @property
    def size(self) -> int:
        """Size of the file in bytes."""
        return self._size

    @property
    def unit_size(self) -> int:
        return self._unit_size

    def __repr__(self) -> str:
        raise NotImplementedError


class DataIO(_InternalIO):
    """Data region IO."""

    def __init__(self, fs: FileSystem, entry: Entry = None):
        self._volume = fs.volume
        self._lss = fs.volume.sector_size.logical
        self._cluster_size = fs.boot_sector.cluster_size
        self._cluster_size_bytes = self._cluster_size * self._lss
        self._total_clusters = fs.boot_sector.total_clusters
        self._region_start = fs.boot_sector.data_region_start
        self._region_size = fs.boot_sector.data_region_size
        self._fat = fs.fat

        # unit: cluster, position passed as index of cluster chain
        self._pos = 0
        self._unit_size = self._cluster_size_bytes

        if entry is None:
            # use root directory
            self._chain = list(fs.fat.get_chain(2))
        else:
            start_cluster = entry.cluster(fat_32=fs.type is FsType.FAT_32)
            self._chain = list(fs.fat.get_chain(start_cluster))

        if entry is None or Attributes.SUBDIRECTORY in entry.attributes:
            self._size = len(self._chain) * self._cluster_size_bytes
        else:
            self._size = entry.size  # bytes

        self._fd_count = 0  # count of file descriptors pointing to this stream
        self._last_read: datetime | None = None
        self._last_write: datetime | None = None

    def _allocate(self, min_size: int) -> int:
        """Allocate as many clusters as needed to provide a minimum file size of
        ``min_size`` bytes.

        Returns how many new clusters were allocated.
        """
        self._check_closed()
        self._check_writable()

        if min_size <= self._size:
            return 0

        clusters_required = (min_size - 1) // self._cluster_size_bytes + 1
        to_allocate = clusters_required - len(self._chain)
        if to_allocate <= 0:
            self._size = min_size
            return 0

        new_clusters = tuple(self._fat.next_free_clusters(to_allocate))  # at least 1

        if len(self._chain) > 0:
            last_cluster = self._chain[-1]
            self._fat[last_cluster] = new_clusters[0]

        # build cluster chain
        last = new_clusters[0]
        for cluster in new_clusters[1:]:
            self._fat[last] = cluster
            last = cluster

        self._fat.set_eoc(new_clusters[-1])  # end of chain
        self._fat.flush()

        old_chain_len = len(self._chain)
        self._chain.extend(new_clusters)
        zero_cluster = b'\x00' * self._cluster_size_bytes

        for cluster_index in range(old_chain_len, clusters_required):
            self._write_units(cluster_index, zero_cluster)

        self._size = min_size
        return to_allocate

    def _free(self, max_size: int) -> int:
        """Free as many clusters as needed to guarantee a maximum file size of
        ``max_size`` bytes.

        Returns how many clusters were freed.
        """
        self._check_closed()
        self._check_writable()

        if max_size < 0:
            raise ValueError(f'Invalid maximum file size {max_size}')
        if max_size >= self._size:
            return 0

        clusters_required = (max_size - 1) // self._cluster_size_bytes + 1
        to_free = len(self._chain) - clusters_required
        if to_free <= 0:
            self._size = max_size
            return 0

        old_clusters = self._chain[-to_free:]
        new_chain = self._chain[:-to_free]

        if len(new_chain) > 0:
            last_cluster = new_chain[-1]
            self._fat.set_eoc(last_cluster)

        for cluster in old_clusters:
            self._fat.set_empty(cluster)

        self._fat.flush()
        self._chain = new_chain
        self._size = max_size
        return to_free

    def _check_cluster(self, cluster: int) -> None:
        if not 0 <= (cluster - 2) < self._total_clusters:
            raise ValueError(f'Invalid cluster number {cluster} in chain')

    def _read_units(self, pos: int, count: int) -> bytes:
        """Read ``count`` clusters starting at cluster with chain index ``pos``."""
        if pos < 0:
            raise ValueError('Start cluster index must be greater than or equal to 0')
        if count <= 0:
            raise ValueError('Cluster count must be greater than 0')
        if pos + count > len(self._chain):
            raise ValueError('Not enough clusters in chain to read from')

        b = b''
        for cluster in self._chain[pos : pos + count]:
            self._check_cluster(cluster)
            start_sector = self._region_start + (cluster - 2) * self._cluster_size
            cluster_bytes = self._volume.read_at(start_sector, self._cluster_size)
            b += cluster_bytes

        self._last_read = datetime.now()
        return b

    def _write_units(self, pos: int, b: bytes | memoryview) -> None:
        """Write ``b`` to file starting at cluster ``pos``."""
        if pos < 0:
            raise ValueError('Start cluster index must be greater than or equal to 0')
        if len(b) % self._cluster_size_bytes != 0:
            raise ValueError(
                f'Bytes object to write must be a multiple of '
                f'{self._cluster_size_bytes} long, got {len(b)} bytes'
            )
        count = len(b) // self._cluster_size_bytes
        if count <= 0:
            return
        if pos + count > len(self._chain):
            raise ValueError('Not enough clusters in chain to write to')

        clusters = self._chain[pos : pos + count]
        b_offsets = range(0, len(b), self._cluster_size_bytes)

        for cluster, b_offset in zip(clusters, b_offsets):
            self._check_cluster(cluster)
            b_part = b[b_offset : b_offset + self._cluster_size_bytes]
            start_sector = self._region_start + (cluster - 2) * self._cluster_size
            self._volume.write_at(start_sector, b_part)

        self._last_write = datetime.now()

    def increment_fd_count(self) -> None:
        self._fd_count += 1

    def decrement_fd_count(self) -> None:
        if self._fd_count <= 0:
            raise ValueError('Count of file descriptors cannot be less than 0')
        self._fd_count -= 1

    @property
    def fd_count(self) -> int:
        return self._fd_count

    @property
    def last_read(self) -> datetime | None:
        return self._last_read

    @property
    def last_write(self) -> datetime | None:
        return self._last_write

    @property
    def start_cluster(self) -> int:
        if len(self._chain) > 0:
            return self._chain[0]
        return 0

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(chain={self._chain}, size={self._size})'


class RootdirIO(_InternalIO):
    """Root directory region IO."""

    def __init__(self, fs: FileSystem):
        self._volume = fs.volume
        self._lss = fs.volume.sector_size.logical
        self._start = fs.boot_sector.rootdir_region_start

        # unit: logical sector, position passed as index of sector
        self._pos = 0
        self._unit_size = self._lss
        self._size = fs.boot_sector.rootdir_region_size * self._lss  # bytes

    def _allocate(self, min_size: int) -> int:
        self._check_closed()
        self._check_writable()
        if min_size > self._size:
            raise FileSystemLimit('Maximum capacity of root directory reached')
        return 0

    def _free(self, max_size: int) -> int:
        raise ValueError('Root directory region cannot be truncated')

    def _read_units(self, pos: int, count: int) -> bytes:
        return self._volume.read_at(self._start + pos, count)

    def _write_units(self, pos: int, b: bytes | memoryview) -> None:
        return self._volume.write_at(self._start + pos, b)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(size={self._size})'
