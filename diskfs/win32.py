"""Platform-specific disk operations for Windows systems."""

from __future__ import annotations

import sys

assert sys.platform == 'win32'  # skipcq: BAN-B101

# noinspection PyCompatibility
import msvcrt
from ctypes import (
    Array,
    Structure,
    WinError,
    byref,
    c_char,
    c_char_p,
    c_uint,
    create_string_buffer,
    sizeof,
    windll,
)
from ctypes.wintypes import (
    BOOL,
    BOOLEAN,
    BYTE,
    DWORD,
    HANDLE,
    LARGE_INTEGER,
    LPDWORD,
    LPVOID,
    WPARAM,
)
from typing import TypeVar

from .base import DeviceProperties, SectorSize
from .typing import StrPath

__all__ = [
    'device_io_control',
    'device_size',
    'device_sector_size',
    'reread_partition_table',
]


PVOID = LPVOID
ULONG_PTR = WPARAM  # same behavior as ULONG_PTR

PARAM_IN = 1
PARAM_OUT = 2

IOCTL_DISK_GET_LENGTH_INFO = 475228
IOCTL_DISK_GET_DRIVE_GEOMETRY = 458752
IOCTL_DISK_UPDATE_PROPERTIES = 459072
IOCTL_STORAGE_QUERY_PROPERTY = 2954240

# IOCTL_STORAGE_QUERY_PROPERTY
STORAGE_DEVICE_PROPERTY = 0  # value of enum STORAGE_PROPERTY_ID
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
class STORAGE_DESCRIPTOR_HEADER(Structure):
    """Possible output structure for ``IOCTL_STORAGE_QUERY_PROPERTY`` when requesting
    ``StorageDeviceProperty``.

    Only used to determine the required buffer size for ``STORAGE_DEVICE_DESCRIPTOR``.
    """

    _fields_ = [
        ('Version', DWORD),
        ('Size', DWORD),
    ]


# noinspection PyPep8Naming
def STORAGE_DEVICE_DESCRIPTOR(size: int) -> type[Structure]:
    """Return the output structure for ``IOCTL_STORAGE_QUERY_PROPERTY`` of size
    ``size`` when requesting ``StorageDeviceProperty``.
    """

    def with_raw_properties_size(raw_properties_size: int) -> type[Structure]:
        """Return the ``STORAGE_DEVICE_DESCRIPTOR`` structure with the field
        ``RawDeviceProperties`` being of size ``raw_properties_size``.
        """
        # noinspection PyPep8Naming
        class STORAGE_DEVICE_DESCRIPTOR_(Structure):
            """``STORAGE_DEVICE_DESCRIPTOR`` structure of dynamic size."""

            _fields_ = [
                ('Version', DWORD),
                ('Size', DWORD),
                ('DeviceType', BYTE),
                ('DeviceTypeModifier', BYTE),
                ('RemovableMedia', BOOLEAN),
                ('CommandQueueing', BOOLEAN),
                ('VendorIdOffset', DWORD),
                ('ProductIdOffset', DWORD),
                ('ProductRevisionOffset', DWORD),
                ('SerialNumberOffset', DWORD),
                ('BusType', c_uint),  # enum STORAGE_BUS_TYPE
                ('RawPropertiesLength', DWORD),
                ('RawDeviceProperties', BYTE * raw_properties_size),
            ]

        return STORAGE_DEVICE_DESCRIPTOR_

    size_without_raw_properties = sizeof(with_raw_properties_size(0))
    if size < size_without_raw_properties:
        raise ValueError(
            f'Size must be greater than or equal to size without raw properties block '
            f'(got {size} instead of at least {size_without_raw_properties} bytes)'
        )

    return with_raw_properties_size(size - size_without_raw_properties)


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


def device_io_control(
    fd: int,
    control_code: int,
    in_buffer: Array[c_char] = None,
    out_buffer: Array[c_char] = None,
) -> None:
    """Send a control code directly to a specified device driver, causing the
    corresponding device to perform the corresponding operation.

    Wrapper for ``DeviceIoControl()``.
    """
    handle = msvcrt.get_osfhandle(fd)
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


_S = TypeVar('_S', bound=Structure)


def storage_query_property(
    fd: int,
    storage_property_query: STORAGE_PROPERTY_QUERY,
    out_structure_type: type[_S],
) -> _S:
    """Wrapper for calling ``DeviceIoControl()`` with ``IOCTL_STORAGE_QUERY_PROPERTY``.

    :param fd: File descriptor for the block device.
    :param storage_property_query: ``STORAGE_PROPERTY_QUERY`` to use as the input.
    :param out_structure_type: ``Structure`` subclass used to parse the output.

    Returns the parsed output as an instance of ``out_structure_type``.
    """
    # noinspection PyTypeChecker
    in_buffer = create_string_buffer(bytes(storage_property_query))
    out_buffer = create_string_buffer(sizeof(out_structure_type))
    device_io_control(fd, IOCTL_STORAGE_QUERY_PROPERTY, in_buffer, out_buffer)
    return out_structure_type.from_buffer_copy(out_buffer)


# noinspection PyUnusedLocal
def device_properties(fd: int, path: StrPath) -> DeviceProperties:
    """Return additional properties of a block device.

    :param fd: File descriptor for the block device.
    :param path: Path of the block device.
    """
    query = STORAGE_PROPERTY_QUERY(
        PropertyId=STORAGE_DEVICE_PROPERTY,
        QueryType=PROPERTY_STANDARD_QUERY,
        AdditionalParameters=(BYTE * 1)(0),
    )
    header = storage_query_property(fd, query, STORAGE_DESCRIPTOR_HEADER)
    storage_device_descriptor = STORAGE_DEVICE_DESCRIPTOR(header.Size)
    properties = storage_query_property(fd, query, storage_device_descriptor)

    def unpack_ascii_string(offset: int) -> str | None:
        """Unpack ASCII string starting at byte ``offset`` in ``out_buffer`` and
        ``rstrip()`` the result.

        Returns ``None`` if ``offset`` is 0 as this implies that the regarding
        property is unavailable.
        """
        if offset == 0:
            return None
        # noinspection PyTypeChecker
        null_terminated = bytes(properties)[offset:]
        string_bytes = c_char_p(null_terminated).value  # cut off after \x00

        if string_bytes is None:
            return None
        return string_bytes.decode('ascii').rstrip()

    removable = bool(properties.RemovableMedia)
    vendor = unpack_ascii_string(properties.VendorIdOffset)
    model = unpack_ascii_string(properties.ProductIdOffset)

    return DeviceProperties(removable, vendor, model)


def device_size(fd: int) -> int:
    """Return the size of a block device.

    :param fd: File descriptor for the block device.
    """
    out_buffer = create_string_buffer(sizeof(GET_LENGTH_INFORMATION))
    device_io_control(fd, IOCTL_DISK_GET_LENGTH_INFO, out_buffer=out_buffer)
    length_information = GET_LENGTH_INFORMATION.from_buffer_copy(out_buffer)
    return length_information.Length


def device_sector_size(fd: int) -> SectorSize:
    """Return the logical and physical sector size of a block device.

    :param fd: File descriptor for the block device.
    """
    query = STORAGE_PROPERTY_QUERY(
        PropertyId=STORAGE_ACCESS_ALIGNMENT_PROPERTY,
        QueryType=PROPERTY_STANDARD_QUERY,
        AdditionalParameters=(BYTE * 1)(0),
    )
    alignment = storage_query_property(fd, query, STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR)
    return SectorSize(alignment.BytesPerLogicalSector, alignment.BytesPerPhysicalSector)


def reread_partition_table(fd: int) -> None:
    """Update the operating system's view of a block device's partition table.

    :param fd: File descriptor for the block device.
    """
    device_io_control(fd, IOCTL_DISK_UPDATE_PROPERTIES)
