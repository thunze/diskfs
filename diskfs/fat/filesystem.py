"""FAT implementation of the ``FileSystem`` protocol."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from errno import EACCES, EBADF, EEXIST, EISDIR, ENOENT, ENOTDIR, ENOTEMPTY
from functools import wraps
from io import SEEK_CUR, SEEK_END, BufferedRandom, BufferedReader, UnsupportedOperation
from itertools import count
from os import stat_result
from stat import S_IFDIR, S_IFREG, S_ISDIR, S_ISREG
from threading import Lock
from typing import (
    TYPE_CHECKING,
    Callable,
    Generator,
    Iterator,
    Literal,
    NamedTuple,
    TypeVar,
    overload,
)

from typing_extensions import Concatenate, ParamSpec

from ..base import ValidationError
from ..filesystem import CLUSTER_SIZE_DEFAULT, FsType, StatusFlags, parse_flags
from ..filesystem import FileSystem as FileSystemBase
from .base import FatType
from .directory import (
    ENTRY_SIZE,
    Attributes,
    EightDotThreeEntry,
    Entry,
    Hint,
    create_entry,
    entry_match,
    iter_entries,
    updated_entry,
)
from .fat import Fat
from .io import DataIO, RootdirIO
from .path import Path, PurePath
from .reserved import BootSector

if TYPE_CHECKING:
    from ..typing import ReadableBuffer, StrPath
    from ..volume import Volume

__all__ = ["FileSystem"]


log = logging.getLogger(__name__)


DELETED_BYTE = Hint.DELETED.value.to_bytes(1, "little")
# FILLER_BYTE = b'\xF6'  TODO
# FILLER_BYTE_FLASH = b'\xFF'

MIN_VOLUME_SIZE_READ = 4  # assuming only 1 FAT is present
MIN_VOLUME_SIZE_CREATE = 5

PERMISSIONS_DIR = 0o777
PERMISSIONS_FILE = 0o666


# Typing
P = ParamSpec("P")
R = TypeVar("R")  # return type


@dataclass
class Node:
    entry: Entry
    children: list[Node] | None = None  # None == not parsed yet
    in_use: bool = False

    @property
    def is_directory(self) -> bool:
        return Attributes.SUBDIRECTORY in self.entry.attributes

    @property
    def children_parsed(self) -> list[Node]:
        if self.children is None:
            raise RuntimeError("Node children not parsed yet")
        return self.children


@dataclass
class Root:
    entry: None = None
    children: list[Node] | None = None  # None == not parsed yet

    @property
    def is_directory(self) -> bool:
        return True

    @property
    def children_parsed(self) -> list[Node]:
        if self.children is None:
            raise RuntimeError("Root children not parsed yet")
        return self.children


class FdTableRow(NamedTuple):
    stream: DataIO
    status_flags: StatusFlags
    path: PurePath


class DirEntry:
    """Class with functionality similar to ``os.DirEntry``."""

    def __init__(
        self, fs: FileSystem, dirpath: PurePath, filename: str, stat: stat_result
    ):
        self._fs = fs
        self._dirpath = dirpath
        self._filename = filename
        self._path = dirpath / filename
        self._stat = stat

    @property
    def name(self) -> str:
        return self._filename

    @property
    def path(self) -> str:
        return str(self._path)

    def inode(self) -> int:
        return self._stat.st_ino

    # noinspection PyUnusedLocal
    def is_dir(self, *, follow_symlinks: bool = True) -> bool:
        return S_ISDIR(self._stat.st_mode)

    # noinspection PyUnusedLocal
    def is_file(self, *, follow_symlinks: bool = True) -> bool:
        return S_ISREG(self._stat.st_mode)

    # noinspection PyMethodMayBeStatic
    def is_symlink(self) -> bool:
        return False

    # noinspection PyUnusedLocal
    def stat(self, *, follow_symlinks: bool = True) -> stat_result:
        return self._stat

    def __fspath__(self) -> str:
        return self._path.__fspath__()


def _check_directory(node: Node | Root, *, hint: StrPath) -> None:
    """Ensure that ``node`` is a directory node.

    :param hint: Path shown as a hint in the exception.
    """
    if not node.is_directory:
        raise OSError(ENOTDIR, os.strerror(ENOTDIR), str(hint))


def _check_file(node: Node | Root, *, hint: StrPath) -> None:
    """Ensure that ``node`` is a regular file node.

    :param hint: Path shown as a hint in the exception.
    """
    if node.is_directory:
        raise OSError(EISDIR, os.strerror(EISDIR), str(hint))


def locked(
    method: Callable[Concatenate["FileSystem", P], R],
) -> Callable[Concatenate["FileSystem", P], R]:
    @wraps(method)
    def locked_wrapper(self: "FileSystem", *args: P.args, **kwargs: P.kwargs) -> R:
        with self._lock:
            self._volume.check_closed()
            return method(self, *args, **kwargs)

    return locked_wrapper


# noinspection PyAbstractClass
class FileSystem(FileSystemBase):
    """FAT file system.

    Do not use ``__init__`` directly, use ``FileSystem.create()`` or
    ``FileSystem.from_volume()`` instead.
    """

    def __init__(
        self, volume: Volume, boot_sector: BootSector, fat: Fat, vfat: bool = True
    ):
        self._volume = volume
        self._boot_sector = boot_sector
        self._fat = fat
        self._vfat = vfat
        self._fat_32 = self._boot_sector.fat_type is FatType.FAT_32

        self._cwd = PurePath("/")
        self._root = Root()  # cached directory structure
        self._lock = Lock()

        # File descriptor management
        self._fd_table: dict[int, FdTableRow] = {}

    @classmethod
    def create(
        cls,
        volume: Volume,
        size_lba: int = None,
        *,
        cluster_size: int = CLUSTER_SIZE_DEFAULT,
        label: str = "",
        vfat: bool = True,
    ) -> FileSystem:
        """Create new file system on ``volume``.

        **Caution:** If any file system already resides on the volume, it will be
        overwritten and thus be rendered unusable. Always create a backup of your
        data before formatting a volume.
        """
        if size_lba is None:
            size_lba = volume.size_lba
        if not MIN_VOLUME_SIZE_CREATE <= size_lba <= volume.size_lba:
            raise ValidationError(
                f"File system size must be in range ({MIN_VOLUME_SIZE_CREATE}, "
                f"{volume.size_lba})"
            )
        raise NotImplementedError

    @classmethod
    def from_volume(cls, volume: Volume, *, vfat: bool = True) -> FileSystem:
        """Get file system residing on partition ``partition`` of ``disk``.

        If ``partition`` is not specified, it is tried to parse a standalone file
        system on the unpartitioned disk.
        """
        if volume.size_lba < MIN_VOLUME_SIZE_READ:
            raise ValidationError(
                f"Volume must span at least {MIN_VOLUME_SIZE_READ} logical sectors"
            )

        boot_sector_bytes = volume.read_at(0, 1)
        boot_sector = BootSector.from_bytes(boot_sector_bytes)
        boot_sector.validate_for_volume(volume)

        # fsinfo = None
        # if getattr(boot_sector.bpb, 'fsinfo_available', False):
        #     fsinfo_bytes = volume.read_at(1, 1)
        #     fsinfo = FsInfo.from_bytes(fsinfo_bytes)

        fat = Fat(volume, boot_sector)
        return cls(volume, boot_sector, fat, vfat)

    @property
    def type(self) -> FsType:
        """File system type."""
        return self._boot_sector.fat_type.fs_type

    @property
    def volume(self) -> Volume:
        return self._volume

    @property
    def boot_sector(self) -> BootSector:
        return self._boot_sector

    @property
    def fat(self) -> Fat:
        return self._fat

    @property
    def vfat(self) -> bool:
        return self._vfat

    @property
    def lock(self) -> Lock:
        return self._lock

    def at(self, *args: StrPath) -> Path:
        return Path(*args, fs=self)

    # Helper methods

    def _is_root(self, path: StrPath) -> bool:
        return PurePath(self.realpath(path)) == PurePath("/")

    @overload
    def _scandir(
        self, entry: Entry = None, *, only_useful: Literal[True] = ...
    ) -> Iterator[Entry]:
        ...

    @overload
    def _scandir(
        self, entry: Entry = None, *, only_useful: Literal[False] = ...
    ) -> Iterator[Entry | EightDotThreeEntry]:
        ...

    def _scandir(
        self, entry: Entry = None, *, only_useful: Literal[False, True] = True
    ) -> Iterator[Entry | EightDotThreeEntry]:
        """Yield directory entries found for directory with entry ``entry``.

        If ``entry`` is ``None``, the root directory is scanned.
        """
        if entry is not None:
            cluster = entry.cluster(fat_32=self._fat_32)
            if Attributes.SUBDIRECTORY not in entry.attributes or cluster == 0:
                return  # file or empty directory

        with self._get_internal_io(entry) as stream:
            with BufferedReader(stream, stream.unit_size) as reader:

                def bytes_gen() -> Iterator[bytes]:
                    while True:
                        b = reader.read(ENTRY_SIZE)
                        if not b:
                            return
                        yield b

                yield from iter_entries(
                    bytes_gen(),
                    only_useful=only_useful,
                    vfat=self._vfat,
                )

    def _get_children(self, node: Node | Root) -> Iterator[Node]:
        if node.children is not None:
            yield from node.children
            return

        children = []
        for child_entry in self._scandir(node.entry):
            child = Node(child_entry)
            children.append(child)
            yield child

        node.children = children

    def _find_node(
        self,
        path: StrPath,
        *,
        set_in_use: bool = False,
        unset_in_use: bool = False,
        check_in_use: bool = False,
    ) -> Node:
        if set_in_use + unset_in_use + check_in_use > 1:
            raise ValueError(
                "Can only either set, unset or check whether the in-use flag is set"
            )

        p = PurePath(self.realpath(path))
        node: Node | Root = self._root

        for index, part in enumerate(p.parts[1:]):
            found = False

            # scan directory for entry
            for child in self._get_children(node):
                if entry_match(part, child.entry, vfat=self._vfat):
                    if set_in_use:
                        child.in_use = True
                    if unset_in_use:
                        child.in_use = False
                    if index == len(p.parts) - 2:  # last part, we found the entry
                        # Only check if the leaf is in use.
                        if check_in_use and child.in_use:
                            raise OSError(
                                EACCES,
                                "File or directory is being used by another process",
                                str(path),
                            )
                        return child
                    if child.is_directory:
                        found = True
                        node = child
                    break

            if not found:
                # abort if not found or not a directory
                break

        raise OSError(ENOENT, os.strerror(ENOENT), str(path))  # FileNotFoundError

    def _find_node_or_root(self, path: StrPath) -> Node | Root:
        if self._is_root(path):
            return self._root
        return self._find_node(path)

    def _get_internal_io(self, entry: Entry = None) -> DataIO | RootdirIO:
        """Get a low-level file-like object for a file or a directory."""
        if not self._fat_32 and entry is None:
            return RootdirIO(self)
        else:
            return DataIO(self, entry)

    def _transform_entry(
        self,
        old_entry: Entry | None,
        new_entry: Entry | None,
        parent_entry: Entry | None,
        parent_path: PurePath,
    ) -> None:
        """Replace entry with file name of ``old_entry`` with ``new_entry_`` in
        directory with entry ``parent_entry``.

        If ``old_entry`` is ``None``, ``new_entry`` is created.
        If ``new_entry`` is ``None``, ``old_entry`` is deleted.
        If ``parent_entry`` is ``None``, the root directory is used as the parent
        directory.
        """
        if old_entry == new_entry:
            return

        existing_entries = tuple(self._scandir(parent_entry, only_useful=False))

        # If new entry is None, delete all old entries.
        # If new entry takes up more total entries, delete all old entries and
        # allocate new space in directory table.
        # If new entry takes up less or equal the amount of total entries,
        # delete first entries and overwrite last entries.

        raw = self._get_internal_io(parent_entry)
        buffer = BufferedRandom(raw, raw.unit_size)
        try:
            replaced_old_entry = False

            # delete old entry or parts of it
            if old_entry is not None:
                found_entry = None
                old_entries_start = 0  # total entries

                for entry in existing_entries:
                    if isinstance(entry, Entry):
                        old_entry_filename = old_entry.filename(vfat=self._vfat)
                        if entry_match(old_entry_filename, entry, vfat=self._vfat):
                            found_entry = entry
                            break
                        old_entries_start += entry.total_entries
                    else:
                        old_entries_start += 1  # single EightDotThreeEntry

                if found_entry is None:
                    raise ValueError("Could not find old entry in parent directory")

                old_total_entries = found_entry.total_entries
                if new_entry is not None:
                    new_total_entries = new_entry.total_entries
                else:
                    new_total_entries = 0

                if new_entry is None or new_total_entries > old_total_entries:
                    to_delete = old_total_entries
                else:
                    to_delete = old_total_entries - new_total_entries

                buffer.seek(old_entries_start * ENTRY_SIZE)
                for _ in range(to_delete):
                    buffer.write(DELETED_BYTE)
                    buffer.seek(ENTRY_SIZE - 1, SEEK_CUR)

                # replace with new entry
                if new_entry is not None and new_total_entries <= old_total_entries:
                    buffer.write(bytes(new_entry))
                    replaced_old_entry = True

            # create new entry at end of directory table
            if new_entry is not None and not replaced_old_entry:
                total_entries_directory = sum(
                    entry.total_entries if isinstance(entry, Entry) else 1
                    for entry in existing_entries
                )
                buffer.seek(total_entries_directory * ENTRY_SIZE)
                buffer.write(bytes(new_entry))

        finally:
            buffer.close()
            # update cluster number, size etc. of parent entry
            if not self._is_root(parent_path):
                if not isinstance(raw, DataIO):
                    raise ValueError("Expected DataIO object for non-root parent path")
                self._update_entry_by_stream(raw, parent_path)

    def _create_child(
        self, path: PurePath, parent: Node | Root, *, directory: bool
    ) -> Node:
        existing = [child.entry for child in self._get_children(parent)]
        attributes = Attributes.SUBDIRECTORY if directory else Attributes.ARCHIVE
        now = datetime.now()
        entry = create_entry(
            existing,
            path.name,
            attributes,
            now,
            now,
            now,
            vfat=self._vfat,
            fat_32=self._fat_32,
        )
        node = Node(entry, [])
        self._transform_entry(None, entry, parent.entry, path.parent)
        parent.children_parsed.append(node)
        return node

    def _update_entry_by_stream(self, stream: DataIO, path: PurePath) -> None:
        node = self._find_node(path)
        new_entry = updated_entry(
            node.entry,
            stream.start_cluster,
            stream.size,
            stream.last_read,
            stream.last_write,
            vfat=self._vfat,
            fat_32=self._fat_32,
        )
        parent_path = path.parent
        parent_entry = self._find_node_or_root(parent_path).entry
        self._transform_entry(node.entry, new_entry, parent_entry, path.parent)
        node.entry = new_entry

    def _new_fd(self) -> int:
        """Generate new file descriptor."""
        existing = self._fd_table.keys()
        # We start at 3 to skip the file descriptors usually used for stdout etc.
        return next(fd for fd in count(start=3) if fd not in existing)

    def _find_in_fd_table(self, fd: int) -> FdTableRow:
        if fd >= 3:
            row = self._fd_table.get(fd)
            if row is not None:
                return row
        raise OSError(EBADF, os.strerror(EBADF), fd)

    def _stat_for_entry(self, entry: Entry = None) -> stat_result:
        """Get ``stat_result`` for ``entry``."""
        dev = getattr(self._boot_sector.bpb, "volume_id", 0)
        if entry is None:
            return stat_result((S_IFDIR | PERMISSIONS_DIR, 0, dev, 1, 0, 0, 0, 0, 0, 0))

        if Attributes.SUBDIRECTORY in entry.attributes:
            mode = S_IFDIR | PERMISSIONS_DIR
        else:
            mode = S_IFREG | PERMISSIONS_FILE

        ino = entry.cluster(fat_32=self._fat_32)  # may be zero
        size = entry.size
        atime = 0 if entry.last_accessed is None else entry.last_accessed.timestamp()
        mtime = 0 if entry.last_modified is None else entry.last_modified.timestamp()
        ctime = 0 if entry.created is None else entry.created.timestamp()

        return stat_result((mode, ino, dev, 1, 0, 0, size, atime, mtime, ctime))

    # Low-level IO methods for use with a file descriptor

    @locked
    def openfd(self, path: StrPath, flags: int, mode: int = 0o777) -> int:
        if self._is_root(path):
            raise OSError(EISDIR, os.strerror(EISDIR), str(path))

        realpath = PurePath(self.realpath(path))
        status_flags, creating, exclusive, truncating = parse_flags(flags)
        if status_flags.writable:
            self._volume.check_writable()

        stream = next(
            (row.stream for row in self._fd_table.values() if row.path == realpath),
            None,
        )
        exists = True  # assume the file exists

        if stream is None:
            # not open, but might exist
            parent = self._find_node_or_root(realpath.parent)
            try:
                node = self._find_node(realpath)
            except FileNotFoundError:
                # doesn't exist
                if not creating:
                    raise
                exists = False
                node = self._create_child(realpath, parent, directory=False)
            else:
                # exists
                _check_file(node, hint=realpath)
                exists = True

            stream = DataIO(self, node.entry)
            self._find_node(realpath, set_in_use=True)

        if exists and exclusive:
            raise OSError(EEXIST, os.strerror(EEXIST), str(path))

        fd = self._new_fd()
        self._fd_table[fd] = FdTableRow(stream, status_flags, realpath)
        stream.increment_fd_count()

        if truncating:
            stream.truncate(0)
        return fd

    @locked
    def closefd(self, fd: int) -> None:
        stream, _, path = self._find_in_fd_table(fd)
        self._update_entry_by_stream(stream, path)

        # free file descriptor
        del self._fd_table[fd]
        stream.decrement_fd_count()

        not_in_use = stream.fd_count <= 0
        if not_in_use:
            stream.close()

        self._find_node(path, unset_in_use=not_in_use)

    @locked
    def statfd(self, fd: int) -> stat_result:
        path = self._find_in_fd_table(fd).path
        node = self._find_node(path)
        return self._stat_for_entry(node.entry)

    @locked
    def seekfd(self, fd: int, offset: int, whence: int) -> int:
        stream = self._find_in_fd_table(fd).stream
        return stream.seek(offset, whence)

    @locked
    def readfd(self, fd: int, size: int) -> bytes:
        stream, flags, _ = self._find_in_fd_table(fd)
        if not flags.readable:
            raise UnsupportedOperation("File not open for reading")
        return stream.read(size)

    @locked
    def writefd(self, fd: int, b: ReadableBuffer) -> int:
        # TODO: use FsInfo
        stream, flags, _ = self._find_in_fd_table(fd)
        if not flags.writable:
            raise UnsupportedOperation("File not open for writing")
        if flags.appending:
            stream.seek(0, SEEK_END)
        return stream.write(b)

    @locked
    def truncatefd(self, fd: int, size: int) -> int:
        stream, flags, path = self._find_in_fd_table(fd)
        if not flags.writable:
            raise UnsupportedOperation("File not open for writing")
        res = stream.truncate(size)
        self._update_entry_by_stream(stream, path)
        return res

    @locked
    def flushfd(self, fd: int) -> None:
        stream, _, path = self._find_in_fd_table(fd)
        self._update_entry_by_stream(stream, path)

    @locked
    def isattyfd(self, fd: int) -> bool:
        self._find_in_fd_table(fd)
        return False

    # Standard accessor methods

    @locked
    def stat(self, path: StrPath, *, follow_symlinks: bool = True) -> stat_result:
        node = self._find_node_or_root(path)
        return self._stat_for_entry(node.entry)

    @locked
    def listdir(self, path: StrPath = None) -> list[str]:
        if path is None:
            path = "."
        node = self._find_node_or_root(path)
        _check_directory(node, hint=path)
        return [
            child.entry.filename(vfat=self._vfat) for child in self._get_children(node)
        ]

    def scandir(self, path: StrPath = None) -> Generator[DirEntry, None, None]:
        with self._lock:
            self._volume.check_closed()
            if path is None:
                path = "."
            node = self._find_node_or_root(path)
            _check_directory(node, hint=path)
            realpath = PurePath(self.realpath(path))
            children = self._get_children(node)

        while True:
            try:
                with self._lock:
                    child = next(children)
            except StopIteration:
                break
            else:
                stat = self._stat_for_entry(child.entry)
                filename = child.entry.filename(vfat=self._vfat)
                yield DirEntry(self, realpath, filename, stat)

    @locked
    def mkdir(self, path: StrPath, mode: int = 0o777) -> None:
        self._volume.check_writable()
        realpath = PurePath(self.realpath(path))
        if self._is_root(path):
            raise OSError(EACCES, os.strerror(EACCES), str(path))

        parent = self._find_node_or_root(realpath.parent)
        try:
            self._find_node(realpath)
        except FileNotFoundError:
            pass
        else:
            raise OSError(EEXIST, os.strerror(EEXIST), str(path))

        # doesn't exist yet
        self._create_child(realpath, parent, directory=True)

    @locked
    def rmdir(self, path: StrPath) -> None:
        self._volume.check_writable()
        realpath = PurePath(self.realpath(path))
        if self._is_root(path):
            raise OSError(EACCES, os.strerror(EACCES), str(path))

        node = self._find_node(path, check_in_use=True)
        _check_directory(node, hint=path)
        if next(self._get_children(node), False):  # any children?
            raise OSError(ENOTEMPTY, os.strerror(ENOTEMPTY), str(path))

        parent = self._find_node_or_root(realpath.parent)
        self._transform_entry(node.entry, None, parent.entry, realpath.parent)
        with self._get_internal_io(node.entry) as stream:
            stream.truncate(0)
        parent.children_parsed.remove(node)

    @locked
    def unlink(self, path: StrPath) -> None:
        self._volume.check_writable()
        realpath = PurePath(self.realpath(path))
        if self._is_root(path):
            raise OSError(EACCES, os.strerror(EACCES), str(path))

        node = self._find_node(path, check_in_use=True)
        _check_file(node, hint=path)
        parent = self._find_node_or_root(realpath.parent)
        self._transform_entry(node.entry, None, parent.entry, realpath.parent)
        with self._get_internal_io(node.entry) as stream:
            stream.truncate(0)
        parent.children_parsed.remove(node)

    @locked
    def _move(self, src: StrPath, dst: StrPath, *, replace: bool = False) -> None:
        self._volume.check_writable()
        realsrc = PurePath(self.realpath(src))
        realdst = PurePath(self.realpath(dst))

        if self._is_root(src):
            raise OSError(EACCES, os.strerror(EACCES), str(src))
        if self._is_root(dst):
            raise OSError(EACCES, os.strerror(EACCES), str(dst))
        if realsrc == realdst:
            return

        src_node = self._find_node(src, check_in_use=True)
        directory = src_node.is_directory
        dst_node = None
        try:
            dst_node = self._find_node(dst, check_in_use=True)
        except FileNotFoundError:
            pass
        else:
            if not replace:
                raise OSError(EEXIST, os.strerror(EEXIST), str(dst))
            if directory:
                _check_directory(dst_node, hint=dst)
                if next(self._get_children(dst_node), False):  # any children?
                    raise OSError(EEXIST, os.strerror(EEXIST), str(dst))
            else:
                _check_file(dst_node, hint=dst)

        src_parent = self._find_node_or_root(realsrc.parent)
        dst_parent = self._find_node_or_root(realdst.parent)
        src_entry = src_node.entry
        dst_entry = None if dst_node is None else dst_node.entry

        # We explicitly don't remove the source entry from existing if src_node and
        # dst_node are the same to replicate Windows' behavior.
        existing = [child.entry for child in self._get_children(dst_parent)]
        if dst_entry is not None:
            existing.remove(dst_entry)

        # Replace unparsable datetimes from src_entry with current datetime
        now = datetime.now()
        created, last_accessed, last_modified = map(
            lambda dt: now if dt is None else dt,
            (src_entry.created, src_entry.last_accessed, src_entry.last_modified),
        )

        new_entry = create_entry(
            existing,
            realdst.name,
            src_entry.attributes,
            created,
            last_accessed,
            last_modified,
            src_entry.cluster(fat_32=self._fat_32),
            src_entry.size,
            vfat=self._vfat,
            fat_32=self._fat_32,
        )
        new_node = Node(new_entry, src_node.children)

        # We don't replace the destination entry if it exists, we mark it as deleted.
        self._transform_entry(dst_entry, None, dst_parent.entry, realdst.parent)

        if src_parent is dst_parent:
            self._transform_entry(
                src_entry, new_entry, src_parent.entry, realsrc.parent
            )
        else:
            self._transform_entry(src_entry, None, src_parent.entry, realsrc.parent)
            self._transform_entry(None, new_entry, dst_parent.entry, realdst.parent)

        if dst_node is not None:
            # free space of dst_node
            with self._get_internal_io(dst_entry) as stream:
                stream.truncate(0)
            dst_parent.children_parsed.remove(dst_node)

        src_parent.children_parsed.remove(src_node)
        dst_parent.children_parsed.append(new_node)

    def rename(self, src: StrPath, dst: StrPath) -> None:
        """Rename the file or directory ``src`` to ``dst``.

        If ``dst`` exists, ``FileExistsError`` is raised.

        Note that, in contrast to ``os.rename()``, the behavior of this method is
        consistent over all platforms.
        """
        # noinspection PyTypeChecker
        self._move(src, dst, replace=False)

    def replace(self, src: StrPath, dst: StrPath) -> None:
        """Rename the file or directory ``src`` to ``dst``.

        If ``src`` is a file and ``dst`` is a directory or vice-versa, an
        ``IsADirectoryError`` or a ``NotADirectoryError`` will be raised respectively.
        If ``dst`` is a non-empty directory, ``FileExistsError`` is raised.
        In all other cases, ``dst`` will be replaced silently if it exists.

        Note that, in contrast to ``os.replace()``, the behavior of this method is
        consistent over all platforms.
        """
        # noinspection PyTypeChecker
        self._move(src, dst, replace=True)

    @locked
    def utime(
        self,
        path: StrPath,
        times: tuple[int, int] | tuple[float, float] | None = None,
        *,
        ns: tuple[int, int] = None,
        follow_symlinks: bool = True,
    ) -> None:
        self._volume.check_writable()
        realpath = PurePath(self.realpath(path))
        if self._is_root(path):
            raise OSError(EACCES, os.strerror(EACCES), str(path))

        if times is not None and ns is not None:
            raise ValueError("Cannot specify both times and nanosecond timestamps")

        if times is not None and ns is None:
            last_accessed, last_modified = map(datetime.fromtimestamp, times)

        elif times is None and ns is not None:

            def ns_to_datetime(ns_timestamp: int) -> datetime:
                ss, ns_ = divmod(ns_timestamp, 10**9)
                us = timedelta(microseconds=ns_ // 1000)
                return datetime.fromtimestamp(ss) + us

            last_accessed, last_modified = map(ns_to_datetime, ns)

        else:
            now = datetime.now()
            last_accessed, last_modified = now, now

        node = self._find_node(path, check_in_use=True)
        entry = node.entry
        new_entry = updated_entry(
            entry,
            entry.cluster(fat_32=self._fat_32),
            entry.size,
            last_accessed,
            last_modified,
            vfat=self._vfat,
            fat_32=self._fat_32,
        )
        parent = self._find_node_or_root(realpath.parent)
        self._transform_entry(node.entry, new_entry, parent.entry, realpath.parent)
        node.entry = new_entry

    def chmod(self, mode: int, *, follow_symlinks: bool = True) -> None:
        raise NotImplementedError("chmod() is unsupported for this file system")

    def realpath(self, path: StrPath, *, strict: bool = False) -> str:
        self._volume.check_closed()
        # FAT doesn't support symlinks, so abspath() will do. Just make sure we don't
        # pass a relative path to abspath().
        p = PurePath(path)
        if not p.is_absolute():
            path = self._cwd / p

        # noinspection PyProtectedMember
        return PurePath._flavour.pathmod.abspath(path)  # type: ignore[no-any-return]

    # Current working directory

    def getcwd(self) -> str:
        self._volume.check_closed()
        return str(self._cwd)

    @locked
    def chdir(self, path: StrPath) -> None:
        """Change the current working directory."""
        self._volume.check_writable()
        if self._is_root(path):
            self._cwd = PurePath("/")
        else:
            node = self._find_node(path)
            _check_directory(node, hint=path)
            self._cwd = PurePath(self.realpath(path))

    # Linking

    def link(self, src: StrPath, dst: StrPath, *, follow_symlinks: bool = True) -> None:
        raise NotImplementedError("link() is unsupported for this file system")

    def symlink(
        self, src: StrPath, dst: StrPath, target_is_directory: bool = False
    ) -> None:
        raise NotImplementedError("symlink() is unsupported for this file system")

    def readlink(self, path: StrPath) -> str:
        raise NotImplementedError("readlink() is unsupported for this file system")
