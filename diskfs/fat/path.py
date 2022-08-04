"""``Path`` and ``PurePath`` implementations for FAT file systems."""

from __future__ import annotations

import fnmatch
import pathlib
import re

# noinspection PyUnresolvedReferences, PyProtectedMember
from pathlib import _PosixFlavour  # type: ignore[attr-defined]
from typing import TYPE_CHECKING, Callable, Match

if TYPE_CHECKING:
    from ..typing import StrPath
    from .filesystem import FileSystem

__all__ = ['Path', 'PurePath']


# noinspection PyMethodMayBeStatic
class _Flavour(_PosixFlavour):
    """POSIX flavour, but case-insensitive."""

    is_supported = True  # OS-independent

    def casefold(self, s: str) -> str:
        return s.lower()

    def casefold_parts(self, parts: list[str]) -> list[str]:
        return [p.lower() for p in parts]

    def compile_pattern(
        self, pattern: str
    ) -> Callable[[str, int, int], Match[str] | None]:
        return re.compile(fnmatch.translate(pattern), re.IGNORECASE).fullmatch

    def make_uri(self, path: PurePath) -> str:
        raise NotImplementedError('URIs are unsupported for this file system')


class PurePath(pathlib.PurePosixPath):

    _flavour = _Flavour()


class Path(pathlib.Path, PurePath):

    _accessor: FileSystem

    def __new__(cls, *args: StrPath, fs: FileSystem) -> Path:
        # noinspection PyUnresolvedReferences
        self: Path = cls._from_parts(args)  # type: ignore[attr-defined]
        self._accessor = fs
        return self

    @classmethod
    def cwd(cls) -> Path:
        raise NotImplementedError(
            'Need file system context to get cwd; use FileSystem.getcwd() instead'
        )

    @classmethod
    def home(cls) -> Path:
        raise NotImplementedError(
            'home() is unsupported for file systems accessed from userspace'
        )

    def owner(self) -> str:
        raise NotImplementedError(
            'owner() is unsupported for file systems accessed from userspace'
        )

    def group(self) -> str:
        raise NotImplementedError(
            'group() is unsupported for file systems accessed from userspace'
        )
