"""`FileSystem` protocol and related generalized classes."""

from __future__ import annotations

import os
import sys
import warnings
from enum import Enum
from errno import EISDIR
from io import (
    SEEK_CUR,
    SEEK_END,
    SEEK_SET,
    BufferedRandom,
    BufferedReader,
    BufferedWriter,
    RawIOBase,
    TextIOWrapper,
    UnsupportedOperation,
)
from os import stat_result
from pathlib import Path
from stat import S_ISDIR
from typing import (
    TYPE_CHECKING,
    Callable,
    Iterator,
    Literal,
    NamedTuple,
    Protocol,
    overload,
)

from .typing import (
    BufferedAny,
    OpenBinaryMode,
    OpenBinaryModeReading,
    OpenBinaryModeUpdating,
    OpenBinaryModeWriting,
    OpenTextMode,
)

if TYPE_CHECKING:
    from .typing import ReadableBuffer, StrPath, WriteableBuffer
    from .volume import Volume

if sys.version_info >= (3, 10):
    import io

    # make mypy happy
    text_encoding: Callable[[str | None], str] = getattr(io, "text_encoding")
else:
    # noinspection PyUnusedLocal
    def text_encoding(encoding: str | None, stacklevel: int = 2) -> str | None:
        """A helper function to choose the text encoding.

        Returns `encoding` for Python <3.10.
        """
        return encoding


__all__ = [
    "FileSystem",
    "FileIO",
    "FsType",
    "FileSystemLimit",
    "parse_flags",
    "StatusFlags",
    "CLUSTER_SIZE_DEFAULT",
]


CLUSTER_SIZE_DEFAULT = 16  # logical sectors
DEFAULT_BUFFER_SIZE = 8192  # bytes

O_ACCMODE = os.O_RDONLY | os.O_WRONLY | os.O_RDWR  # not defined on some platforms


class FileSystemLimit(OSError):
    pass


class FsType(Enum):
    """File system type."""

    FAT_12 = 0
    FAT_16 = 1
    FAT_32 = 2


class StatusFlags(NamedTuple):
    readable: bool
    writable: bool
    appending: bool


def parse_flags(flags: int) -> tuple[StatusFlags, bool, bool, bool]:
    """Parse file status and file creation flags (3 each).

    Returns a `tuple` of (`StatusFlags`, creating, exclusive, truncating).
    """
    access = flags & O_ACCMODE
    readable = access in (os.O_RDONLY, os.O_RDWR)
    writable = access in (os.O_WRONLY, os.O_RDWR)
    appending = bool(flags & os.O_APPEND)
    creating = bool(flags & os.O_CREAT)
    exclusive = bool(flags & os.O_EXCL)
    truncating = bool(flags & os.O_TRUNC)

    if exclusive and not creating:
        raise ValueError("O_EXCL can only be used in combination with O_CREAT")
    if creating and not writable:
        raise ValueError("Must be writable for creation")
    if truncating and not writable:
        raise ValueError("Must be writable for truncation")

    return StatusFlags(readable, writable, appending), creating, exclusive, truncating


class FileIO(RawIOBase):
    """Clone of `io.FileIO` for use with an abstract `FileSystem`.

    Implementation based on `_pyio.FileIO`.
    """

    _created = False
    _readable = False
    _writable = False
    _appending = False

    def __init__(self, fs: "FileSystem", path: StrPath, mode: str = "r"):
        if not set(mode) <= set("xrwab+"):
            raise ValueError(f"Invalid mode {mode!r}")
        if sum(c in "rwax" for c in mode) != 1 or mode.count("+") > 1:
            raise ValueError(
                "Must have exactly one of create/read/write/append mode and at most "
                "one plus"
            )
        flags = 0

        if "x" in mode:
            self._created = True
            self._writable = True
            flags = os.O_EXCL | os.O_CREAT
        elif "r" in mode:
            self._readable = True
            flags = 0
        elif "w" in mode:
            self._writable = True
            flags = os.O_CREAT | os.O_TRUNC
        elif "a" in mode:
            self._writable = True
            self._appending = True
            flags = os.O_APPEND | os.O_CREAT

        if "+" in mode:
            self._readable = True
            self._writable = True

        if self._readable and self._writable:
            flags |= os.O_RDWR
        elif self._readable:
            flags |= os.O_RDONLY
        else:
            flags |= os.O_WRONLY

        fd = fs.openfd(path, flags, 0o666)
        try:
            if fd < 0:
                raise OSError("Negative file descriptor")

            stat = fs.statfd(fd)
            if S_ISDIR(stat.st_mode):
                raise OSError(EISDIR, os.strerror(EISDIR), path)

            self._blksize = fs.volume.sector_size.physical
            if self._blksize <= 1:
                self._blksize = DEFAULT_BUFFER_SIZE

            if self._appending:
                fs.seekfd(fd, 0, SEEK_END)

        except BaseException:
            fs.closefd(fd)
            raise

        self._fd = fd
        self._path = path
        self._mode = mode
        self._fs = fs

    def __repr__(self) -> str:
        cls = self.__class__
        return (
            f"{cls.__module__}.{cls.__qualname__}(fs={self._fs!r}, "
            f"path={self._path!r}, fd={self._fd}, closed={self.closed})"
        )

    def _check_closed(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file")

    def _check_readable(self) -> None:
        if not self._readable:
            raise UnsupportedOperation("File not open for reading")

    def _check_writable(self) -> None:
        if not self._writable:
            raise UnsupportedOperation("File not open for writing")

    def read(self, size: int = -1) -> bytes:
        self._check_closed()
        self._check_readable()

        if size < 0:
            return self.readall()
        return self._fs.readfd(self._fd, size)

    def readall(self) -> bytes:
        self._check_closed()
        self._check_readable()

        bufsize = self._blksize
        try:
            pos = self._fs.seekfd(self._fd, 0, SEEK_CUR)
            end = self._fs.statfd(self._fd).st_size
            if end >= pos:
                bufsize = end - pos + 1
        except OSError:
            pass

        result = bytearray()
        while True:
            if bufsize <= len(result):
                bufsize = len(result)
                bufsize += max(bufsize, self._blksize)
            n = bufsize - len(result)
            chunk = self._fs.readfd(self._fd, n)
            if not chunk:
                break
            result += chunk

        return bytes(result)

    def readinto(self, b: WriteableBuffer) -> int:
        mem = memoryview(b).cast("B")
        data = self.read(len(mem))
        actual = len(data)
        mem[:actual] = data
        return actual

    def write(self, b: ReadableBuffer) -> int:
        self._check_closed()
        self._check_writable()
        return self._fs.writefd(self._fd, b)

    def seek(self, pos: int, whence: int = SEEK_SET) -> int:
        self._check_closed()
        return self._fs.seekfd(self._fd, pos, whence)

    def tell(self) -> int:
        return self._fs.seekfd(self._fd, 0, SEEK_CUR)

    def truncate(self, size: int | None = None) -> int:
        self._check_closed()
        self._check_writable()
        if size is None:
            size = self.tell()
        self._fs.truncatefd(self._fd, size)
        return size

    def flush(self) -> None:
        self._check_closed()
        self._fs.flushfd(self._fd)

    def close(self) -> None:
        if not self.closed:
            super().close()
            self._fs.closefd(self._fd)

    def readable(self) -> bool:
        self._check_closed()
        return self._readable

    def writable(self) -> bool:
        self._check_closed()
        return self._writable

    def seekable(self) -> bool:
        self._check_closed()
        return True

    def fileno(self) -> int:
        self._check_closed()
        return self._fd

    def isatty(self) -> bool:
        self._check_closed()
        return self._fs.isattyfd(self._fd)

    @property
    def name(self) -> str:
        return str(self._path)

    @property
    def mode(self) -> str:
        return self._mode


# noinspection PyPropertyDefinition
class DirEntry(Protocol):
    @property
    def name(self) -> str:
        ...

    @property
    def path(self) -> str:
        ...

    def inode(self) -> int:
        ...

    def is_dir(self, *, follow_symlinks: bool = True) -> bool:
        ...

    def is_file(self, *, follow_symlinks: bool = True) -> bool:
        ...

    def is_symlink(self) -> bool:
        ...

    def stat(self, *, follow_symlinks: bool = True) -> stat_result:
        ...

    def __fspath__(self) -> str:
        ...


# noinspection PyPropertyDefinition
class FileSystem(Protocol):
    """File system protocol.

    This protocol provides default implementations for some methods.
    """

    @property
    def type(self) -> FsType:
        ...

    @property
    def volume(self) -> Volume:
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(type={self.type}, volume={self.volume})"

    def at(self, *args: StrPath) -> Path:
        ...

    # Low-level IO methods for use with a file descriptor

    def openfd(self, path: StrPath, flags: int, mode: int = 0o666) -> int:
        ...

    def closefd(self, fd: int) -> None:
        ...

    def statfd(self, fd: int) -> stat_result:
        ...

    def seekfd(self, fd: int, pos: int, whence: int) -> int:
        ...

    def readfd(self, fd: int, size: int) -> bytes:
        ...

    def writefd(self, fd: int, b: ReadableBuffer) -> int:
        ...

    def truncatefd(self, fd: int, size: int) -> int:
        ...

    def flushfd(self, fd: int) -> None:
        ...

    def isattyfd(self, fd: int) -> bool:
        ...

    # Standard accessor methods

    def stat(self, path: StrPath, *, follow_symlinks: bool = True) -> stat_result:
        ...

    def listdir(self, path: StrPath | None = None) -> list[str]:
        ...

    def scandir(self, path: StrPath | None = None) -> Iterator[DirEntry]:
        ...

    def mkdir(self, path: StrPath, mode: int = 0o777) -> None:
        ...

    def rmdir(self, path: StrPath) -> None:
        ...

    def unlink(self, path: StrPath) -> None:
        ...

    def rename(self, src: StrPath, dst: StrPath) -> None:
        ...

    def replace(self, src: StrPath, dst: StrPath) -> None:
        ...

    def utime(
        self,
        path: StrPath,
        times: tuple[int, int] | tuple[float, float] | None = None,
        *,
        ns: tuple[int, int] | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        ...

    def chmod(self, mode: int, *, follow_symlinks: bool = True) -> None:
        ...

    def realpath(self, path: StrPath, *, strict: bool = False) -> str:
        ...

    # Current working directory

    def getcwd(self) -> str:
        ...

    def chdir(self, path: StrPath) -> None:
        ...

    # Linking

    def link(self, src: StrPath, dst: StrPath, *, follow_symlinks: bool = True) -> None:
        ...

    def symlink(
        self, src: StrPath, dst: StrPath, target_is_directory: bool = False
    ) -> None:
        ...

    def readlink(self, path: StrPath) -> str:
        ...

    # Default implementations

    def expanduser(self, path: StrPath) -> str:
        raise NotImplementedError(
            "expanduser() is unsupported for file systems accessed from userspace"
        )

    def touch(self, path: StrPath, mode: int = 0o666, exist_ok: bool = True) -> None:
        if exist_ok:
            # First try to bump modification time
            # Implementation note: GNU touch uses the UTIME_NOW option of
            # the utimensat() / futimens() functions.
            try:
                self.utime(path, None)
            except OSError:
                # Avoid exception chaining
                pass
            else:
                return
        flags = os.O_CREAT | os.O_WRONLY
        if not exist_ok:
            flags |= os.O_EXCL
        file = self.openfd(path, flags, mode)
        self.closefd(file)

    @overload
    def open(
        self,
        path: StrPath,
        mode: OpenTextMode = ...,
        buffering: int = ...,
        encoding: str | None = ...,
        errors: str | None = ...,
        newline: str | None = ...,
    ) -> TextIOWrapper:
        ...

    @overload
    def open(
        self,
        path: StrPath,
        mode: OpenBinaryModeUpdating,
        buffering: Literal[-1, 1] = ...,
        encoding: None = ...,
        errors: None = ...,
        newline: None = ...,
    ) -> BufferedRandom:
        ...

    @overload
    def open(
        self,
        path: StrPath,
        mode: OpenBinaryModeWriting,
        buffering: Literal[-1, 1] = ...,
        encoding: None = ...,
        errors: None = ...,
        newline: None = ...,
    ) -> BufferedWriter:
        ...

    @overload
    def open(
        self,
        path: StrPath,
        mode: OpenBinaryModeReading,
        buffering: Literal[-1, 1] = ...,
        encoding: None = ...,
        errors: None = ...,
        newline: None = ...,
    ) -> BufferedReader:
        ...

    @overload
    def open(
        self,
        path: StrPath,
        mode: OpenBinaryMode,
        buffering: Literal[0],
        encoding: None = ...,
        errors: None = ...,
        newline: None = ...,
    ) -> FileIO:
        ...

    @overload
    def open(
        self,
        path: StrPath,
        mode: OpenBinaryMode,
        buffering: int,
        encoding: None = ...,
        errors: None = ...,
        newline: None = ...,
    ) -> BufferedAny | FileIO:
        ...

    @overload
    def open(
        self,
        path: StrPath,
        mode: str,
        buffering: int = ...,
        encoding: str | None = ...,
        errors: str | None = ...,
        newline: str | None = ...,
    ) -> TextIOWrapper | BufferedAny | FileIO:
        ...

    def open(
        self,
        path: StrPath,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> TextIOWrapper | BufferedAny | FileIO:
        """Open file at `path` and return a stream.

        Raises `OSError` upon failure.

        Based on CPython's `_pyio.open()`.
        """
        modes = set(mode)
        if modes - set("axrwb+tU") or len(mode) > len(modes):
            raise ValueError(f"Invalid mode {mode!r}")

        creating = "x" in modes
        reading = "r" in modes
        writing = "w" in modes
        appending = "a" in modes
        updating = "+" in modes
        text = "t" in modes
        binary = "b" in modes

        if "U" in modes:
            if creating or writing or appending or updating:
                raise ValueError(
                    "Mode 'U' cannot be combined with 'x', 'w', 'a', or '+'"
                )
            warnings.warn("'U' mode is deprecated", DeprecationWarning)
            reading = True

        if text and binary:
            raise ValueError("Cannot have text and binary mode at once")
        if creating + reading + writing + appending > 1:
            raise ValueError("Cannot have read/write/append mode at once")
        if not (creating or reading or writing or appending):
            raise ValueError("Must have exactly one of read/write/append mode")
        if binary and encoding is not None:
            raise ValueError("Binary mode does not take an encoding argument")
        if binary and errors is not None:
            raise ValueError("Binary mode does not take an errors argument")
        if binary and newline is not None:
            raise ValueError("Binary mode does not take a newline argument")
        if binary and buffering == 1:
            warnings.warn(
                "Line buffering (buffering=1) is not supported in binary mode, the "
                "default buffer size will be used",
                RuntimeWarning,
            )

        file_io_mode = (
            (creating and "x" or "")
            + (reading and "r" or "")
            + (writing and "w" or "")
            + (appending and "a" or "")
            + (updating and "+" or "")
        )
        raw = FileIO(self, path, file_io_mode)
        result: FileIO | BufferedAny | TextIOWrapper = raw
        try:
            line_buffering = False
            if buffering == 1 or buffering < 0 and raw.isatty():
                buffering = -1
                line_buffering = True

            if buffering < 0:
                buffering = DEFAULT_BUFFER_SIZE
                if buffering < 0:
                    raise ValueError("Invalid buffering size")

            if buffering == 0:
                if binary:
                    return result
                raise ValueError("Cannot have unbuffered text I/O")

            buffer: BufferedAny
            if updating:
                buffer = BufferedRandom(raw, buffering)
            elif creating or writing or appending:
                buffer = BufferedWriter(raw, buffering)
            elif reading:
                buffer = BufferedReader(raw, buffering)
            else:
                raise ValueError(f"Unknown mode {mode!r}")

            result = buffer
            if binary:
                return result

            encoding = text_encoding(encoding)
            text_io = TextIOWrapper(buffer, encoding, errors, newline, line_buffering)
            result = text_io

            # not a property at runtime
            # noinspection PyPropertyAccess
            text_io.mode = mode  # type: ignore[misc]
            return result

        except BaseException:
            result.close()
            raise
