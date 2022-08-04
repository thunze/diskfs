"""Certain types used across the package."""

from __future__ import annotations

from array import array
from io import BufferedRandom, BufferedReader, BufferedWriter
from mmap import mmap
from os import PathLike
from pickle import PickleBuffer
from typing import TYPE_CHECKING, Any, Literal, Union

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences, PyProtectedMember
    from ctypes import _CData

    ReadOnlyBuffer = bytes
    WriteableBuffer = Union[
        bytearray, memoryview, array[Any], mmap, _CData, PickleBuffer
    ]
    ReadableBuffer = Union[ReadOnlyBuffer, WriteableBuffer]

    StrPath = Union[str, PathLike[str]]


__all__ = [
    'NoneType',
    'StrPath',
    'BufferedAny',
    'ReadOnlyBuffer',
    'WriteableBuffer',
    'ReadableBuffer',
    'OpenTextModeUpdating',
    'OpenTextModeWriting',
    'OpenTextModeReading',
    'OpenTextMode',
    'OpenBinaryModeUpdating',
    'OpenBinaryModeWriting',
    'OpenBinaryModeReading',
    'OpenBinaryMode',
]


NoneType = type(None)

BufferedAny = Union[BufferedRandom, BufferedWriter, BufferedReader]

OpenTextModeUpdating = Literal[
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
OpenTextModeWriting = Literal["w", "wt", "tw", "a", "at", "ta", "x", "xt", "tx"]
OpenTextModeReading = Literal[
    "r", "rt", "tr", "U", "rU", "Ur", "rtU", "rUt", "Urt", "trU", "tUr", "Utr"
]
OpenTextMode = Union[OpenTextModeUpdating, OpenTextModeWriting, OpenTextModeReading]

OpenBinaryModeUpdating = Literal[
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
OpenBinaryModeWriting = Literal["wb", "bw", "ab", "ba", "xb", "bx"]
OpenBinaryModeReading = Literal["rb", "br", "rbU", "rUb", "Urb", "brU", "bUr", "Ubr"]
OpenBinaryMode = Union[
    OpenBinaryModeUpdating, OpenBinaryModeReading, OpenBinaryModeWriting
]
