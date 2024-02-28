"""Certain types used across the package."""

from __future__ import annotations

from io import BufferedRandom, BufferedReader, BufferedWriter
from os import PathLike
from typing import TYPE_CHECKING, Literal, Union

from typing_extensions import Buffer, TypeAlias

__all__ = [
    "NoneType",
    "StrPath",
    "BufferedAny",
    "ReadOnlyBuffer",
    "WriteableBuffer",
    "ReadableBuffer",
    "OpenTextModeUpdating",
    "OpenTextModeWriting",
    "OpenTextModeReading",
    "OpenTextMode",
    "OpenBinaryModeUpdating",
    "OpenBinaryModeWriting",
    "OpenBinaryModeReading",
    "OpenBinaryMode",
]


NoneType: TypeAlias = type(None)

# `PathLike` cannot be subscripted at runtime.
if TYPE_CHECKING:
    StrPath: TypeAlias = Union[str, PathLike[str]]

BufferedAny: TypeAlias = Union[BufferedRandom, BufferedWriter, BufferedReader]

# Unfortunately PEP 688 does not allow us to distinguish read-only and writable buffers.
ReadOnlyBuffer: TypeAlias = Buffer
WriteableBuffer: TypeAlias = Buffer
ReadableBuffer: TypeAlias = Union[ReadOnlyBuffer, WriteableBuffer]

OpenTextModeUpdating: TypeAlias = Literal[
    "r+",
    "+r",
    "rt+",
    "r+t",
    "+rt",
    "tr+",
    "t+r",
    "+tr",
    "w+",
    "+w",
    "wt+",
    "w+t",
    "+wt",
    "tw+",
    "t+w",
    "+tw",
    "a+",
    "+a",
    "at+",
    "a+t",
    "+at",
    "ta+",
    "t+a",
    "+ta",
    "x+",
    "+x",
    "xt+",
    "x+t",
    "+xt",
    "tx+",
    "t+x",
    "+tx",
]
OpenTextModeWriting: TypeAlias = Literal[
    "w", "wt", "tw", "a", "at", "ta", "x", "xt", "tx"
]
OpenTextModeReading: TypeAlias = Literal[
    "r", "rt", "tr", "U", "rU", "Ur", "rtU", "rUt", "Urt", "trU", "tUr", "Utr"
]
OpenTextMode: TypeAlias = Union[
    OpenTextModeUpdating, OpenTextModeWriting, OpenTextModeReading
]

OpenBinaryModeUpdating: TypeAlias = Literal[
    "rb+",
    "r+b",
    "+rb",
    "br+",
    "b+r",
    "+br",
    "wb+",
    "w+b",
    "+wb",
    "bw+",
    "b+w",
    "+bw",
    "ab+",
    "a+b",
    "+ab",
    "ba+",
    "b+a",
    "+ba",
    "xb+",
    "x+b",
    "+xb",
    "bx+",
    "b+x",
    "+bx",
]
OpenBinaryModeWriting: TypeAlias = Literal["wb", "bw", "ab", "ba", "xb", "bx"]
OpenBinaryModeReading: TypeAlias = Literal[
    "rb", "br", "rbU", "rUb", "Urb", "brU", "bUr", "Ubr"
]
OpenBinaryMode: TypeAlias = Union[
    OpenBinaryModeUpdating, OpenBinaryModeReading, OpenBinaryModeWriting
]
