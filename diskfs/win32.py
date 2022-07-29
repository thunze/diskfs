"""Platform-specific disk operations for Windows systems."""

import sys

assert sys.platform == 'win32'  # skipcq: BAN-B101

import msvcrt
from ctypes import (
    Array,
    Structure,
    WinError,
    byref,
    c_char,
    c_uint,
    create_string_buffer,
    memmove,
    sizeof,
    windll,
)
from ctypes.wintypes import (
    BOOL,
    BYTE,
    DWORD,
    HANDLE,
    LARGE_INTEGER,
    LPDWORD,
    LPVOID,
    WPARAM,
)
from typing import BinaryIO

from .base import SectorSize

__all__ = ['device_size', 'device_sector_size', 'reread_partition_table']


PVOID = LPVOID
ULONG_PTR = WPARAM  # same behavior as ULONG_PTR

PARAM_IN = 1
PARAM_OUT = 2

IOCTL_DISK_GET_LENGTH_INFO = 475228
IOCTL_DISK_GET_DRIVE_GEOMETRY = 458752
IOCTL_STORAGE_QUERY_PROPERTY = 2954240

# IOCTL_STORAGE_QUERY_PROPERTY
STORAGE_ACCESS_ALIGNMENT_PROPERTY = 6  # value of enum STORAGE_PROPERTY_ID
PROPERTY_STANDARD_QUERY = 0  # value of enum STORAGE_QUERY_TYPE


# noinspection PyPep8Naming
class GET_LENGTH_INFORMATION(Structure):
    """Input structure for ``IOCTL_DISK_GET_LENGTH_INFO``."""

    _fields_ = [('Length', LARGE_INTEGER)]
    Length: int


# noinspection PyPep8Naming
class STORAGE_PROPERTY_QUERY(Structure):
    """Input structure for ``IOCTL_STORAGE_QUERY_PROPERTY``."""

    _fields_ = [
        ('PropertyId', c_uint),  # enum STORAGE_PROPERTY_ID
        ('QueryType', c_uint),  # enum STORAGE_QUERY_TYPE
        ('AdditionalParameters', BYTE * 1),
    ]


# noinspection PyPep8Naming
class STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR(Structure):
    """Output structure for ``IOCTL_STORAGE_QUERY_PROPERTY`` when requesting
    ``StorageAccessAlignmentProperty``.
    """

    _fields_ = [
        ('Version', DWORD),
        ('Size', DWORD),
        ('BytesPerCacheLine', DWORD),
        ('BytesOffsetForCacheAlignment', DWORD),
        ('BytesPerLogicalSector', DWORD),
        ('BytesPerPhysicalSector', DWORD),
        ('BytesOffsetForSectorAlignment', DWORD),
    ]


_DeviceIoControl = windll.kernel32.DeviceIoControl
# Last parameter usually has type LPOVERLAPPED, but we choose LPVOID here because
# we won't open any file with FILE_FLAG_OVERLAPPED anyway.
_DeviceIoControl.argtypes = [
    HANDLE,
    DWORD,
    LPVOID,
    DWORD,
    LPVOID,
    DWORD,
    LPDWORD,
    LPVOID,
]
_DeviceIoControl.restype = BOOL


def _device_io_control(
    file: BinaryIO,
    control_code: int,
    in_buffer: Array[c_char] = None,
    out_buffer: Array[c_char] = None,
) -> None:
    """Send a control code directly to a specified device driver, causing the
    corresponding device to perform the corresponding operation.

    Wrapper for ``DeviceIoControl``.
    """
    handle = msvcrt.get_osfhandle(file.fileno())
    in_buffer_size = len(in_buffer) if in_buffer is not None else 0
    out_buffer_size = len(out_buffer) if out_buffer is not None else 0
    result_buffer = DWORD()

    res = _DeviceIoControl(
        handle,
        control_code,
        in_buffer,
        in_buffer_size,
        out_buffer,
        out_buffer_size,
        byref(result_buffer),
        None,
    )
    if not res:
        raise WinError()


def device_size(file: BinaryIO) -> int:
    """Return the size of a block device.

    :param file: IO handle for the block device.
    """
    length_information = GET_LENGTH_INFORMATION()
    out_buffer = create_string_buffer(sizeof(length_information))
    _device_io_control(file, IOCTL_DISK_GET_LENGTH_INFO, out_buffer=out_buffer)
    memmove(byref(length_information), out_buffer[:], len(out_buffer))  # type: ignore
    length = length_information.Length
    return length


def device_sector_size(file: BinaryIO) -> SectorSize:
    """Get logical and physical sector size for a block device.

    :param file: IO handle for the block device.
    """
    query = STORAGE_PROPERTY_QUERY(
        PropertyId=STORAGE_ACCESS_ALIGNMENT_PROPERTY,
        QueryType=PROPERTY_STANDARD_QUERY,
        AdditionalParameters=(BYTE * 1)(0),
    )
    alignment_descriptor = STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR()
    # noinspection PyTypeChecker
    in_buffer = create_string_buffer(bytes(query))
    out_buffer = create_string_buffer(sizeof(alignment_descriptor))

    _device_io_control(file, IOCTL_STORAGE_QUERY_PROPERTY, in_buffer, out_buffer)
    memmove(byref(alignment_descriptor), out_buffer[:], len(out_buffer))  # type: ignore
    return SectorSize(
        alignment_descriptor.BytesPerLogicalSector,
        alignment_descriptor.BytesPerPhysicalSector,
    )


# skipcq: PYL-W0613
# noinspection PyUnusedLocal
def reread_partition_table(file: BinaryIO) -> None:
    """Force kernel to re-read the partition table on a block device."""
