"""Platform-specific disk operations for Darwin systems."""

from __future__ import annotations

import sys

assert sys.platform == 'darwin'  # skipcq: BAN-B101

import os
from contextlib import contextmanager
from ctypes import c_uint32, c_uint64, create_string_buffer
from fcntl import ioctl
from typing import BinaryIO, Iterator, TypeVar, cast

from ._darwin import (
    ENCODING_UTF_8,
    MODEL_KEY,
    REMOVABLE_KEY,
    VENDOR_KEY,
    CFBoolean,
    CFBooleanGetValue,
    CFDictionaryGetValue,
    CFRelease,
    CFString,
    CFStringGetCString,
    CFStringGetLength,
    CFStringGetMaximumSizeForEncoding,
    CFTypeRef,
    DADiskCopyDescription,
    DADiskCreateFromBSDName,
    DASessionCreate,
)
from .base import DeviceProperties, SectorSize

__all__ = ['device_size', 'device_sector_size', 'reread_partition_table']


DKIOCGETBLOCKSIZE = 0x40046418
DKIOCGETPHYSICALBLOCKSIZE = 0x4004644D
DKIOCGETBLOCKCOUNT = 0x40086419


_T = TypeVar('_T', bound=CFTypeRef)


@contextmanager
def _releasing(cf_object: _T | None) -> Iterator[_T]:
    """Context manager freeing the resources occupied by ``cf_object`` on
    ``__exit__()`` via ``CFRelease()``.

    This context manager should be used on every ``CFTypeRef`` returned by any Darwin
    framework function whose name contains ``Create`` or ``Copy``.

    A ``cf_object`` argument of ``None`` will raise ``ValueError`` on ``__enter__()``
    as a last resort to avoid segmentation fault when calling ``CFRelease()``.
    """
    if cf_object is None:
        raise ValueError('Cannot release null reference')
    try:
        yield cf_object
    finally:
        CFRelease(cf_object)


def _unpack_cf_boolean(boolean: CFBoolean | None) -> bool | None:
    """Convert a ``CFBoolean`` instance to ``bool``.

    Returns ``None`` if ``boolean`` is ``None``.
    """
    if boolean is None:
        return None
    return CFBooleanGetValue(boolean)


def _unpack_cf_string(string: CFString | None) -> str | None:
    """Convert a ``CFString`` instance to ``str``.

    Returns ``None`` if ``string`` is ``None``.
    """
    if string is None:
        return None

    # Encode to get a C string and decode again to get a Python str, both using UTF-8
    # encoding. This is a bit unfortunate but necessary since we have to bridge
    # between CoreFoundation, C and Python types.
    char_count = CFStringGetLength(string)
    max_size = CFStringGetMaximumSizeForEncoding(char_count, ENCODING_UTF_8) + 1
    buffer = create_string_buffer(max_size)

    if not CFStringGetCString(string, buffer, max_size, ENCODING_UTF_8):
        raise RuntimeError("Failed to convert CFString to C string")
    return buffer.value.decode('utf-8')


def device_properties(file: BinaryIO) -> DeviceProperties:
    """Return additional properties of a block device.

    :param file: IO handle for the block device.
    """
    with _releasing(DASessionCreate(None)) as session:
        bsd_name = os.fsencode(file.name)
        disk = DADiskCreateFromBSDName(None, session, bsd_name)

    with _releasing(disk) as disk:
        description = DADiskCopyDescription(disk)
        if description is None:
            return DeviceProperties(None, None, None)

    with _releasing(description):
        removable_ref = CFDictionaryGetValue(description, REMOVABLE_KEY)
        vendor_ref = CFDictionaryGetValue(description, VENDOR_KEY)
        model_ref = CFDictionaryGetValue(description, MODEL_KEY)

        removable = _unpack_cf_boolean(cast(CFBoolean, removable_ref))
        vendor = _unpack_cf_string(cast(CFString, vendor_ref))
        model = _unpack_cf_string(cast(CFString, model_ref))

    if vendor is not None:
        vendor = vendor.rstrip()
    if model is not None:
        model = model.rstrip()

    return DeviceProperties(removable, vendor, model)


def device_size(file: BinaryIO) -> int:
    """Return the size of a block device.

    :param file: IO handle for the block device.
    """
    sector_size, sector_count = c_uint32(), c_uint64()  # see disk.h
    ioctl(file, DKIOCGETBLOCKSIZE, sector_size)
    ioctl(file, DKIOCGETBLOCKCOUNT, sector_count)
    return sector_size.value * sector_count.value


def device_sector_size(file: BinaryIO) -> SectorSize:
    """Return the logical and physical sector size of a block device.

    :param file: IO handle for the block device.
    """
    logical, physical = c_uint32(), c_uint32()  # see disk.h
    ioctl(file, DKIOCGETBLOCKSIZE, logical)
    ioctl(file, DKIOCGETPHYSICALBLOCKSIZE, physical)
    return SectorSize(logical.value, physical.value)


# skipcq: PYL-W0613
# noinspection PyUnusedLocal
def reread_partition_table(file: BinaryIO) -> None:
    """Force kernel to re-read the partition table on a block device."""
