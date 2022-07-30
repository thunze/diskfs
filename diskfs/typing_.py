"""Certain types used across the package."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from array import array

    # noinspection PyUnresolvedReferences, PyProtectedMember
    from ctypes import _CData
    from mmap import mmap
    from pickle import PickleBuffer

    ReadOnlyBuffer = bytes
    WriteableBuffer = bytearray | memoryview | array[Any] | mmap | _CData | PickleBuffer
    ReadableBuffer = ReadOnlyBuffer | WriteableBuffer


__all__ = ['ReadOnlyBuffer', 'WriteableBuffer', 'ReadableBuffer']
