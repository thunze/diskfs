"""MBR partitioning.

See https://en.wikipedia.org/wiki/Master_boot_record.
See https://wiki.osdev.org/Partition_Table.
"""

import struct
from enum import IntEnum
from typing import TYPE_CHECKING

from .._base import ParseError, SectorSize
from ._base import TableType, check_alignment, check_bounds, check_overlapping

if TYPE_CHECKING:
    from ..disk import Disk

__all__ = ['Table', 'PartitionEntry', 'PartitionType']


MIN_LSS = 512  # minimum logical sector size required for MBR partitioning

BOOT_CODE_SIZE = 446
PARTITION_ENTRIES_START = 446
PARTITION_ENTRIES_COUNT = 4
SIGNATURE_START = 510

SIGNATURE = b'\x55\xaa'
STATUS_ACTIVE = 0x80
STATUS_INACTIVE = 0x00

# CHS addressing
HEAD_INVALID = 255
SECTOR_INVALID = 0
CHS_INVALID = (1023, 254, 63)  # single address also considered invalid
CHS_OVERFLOW = (1023, 255, 63)  # used instead of addresses > CHS_MAX_ADDRESSABLE

# circa 8 GiB for disks with a logical sector size of 512 bytes
# -1 because (1023, 254, 63) is also considered invalid
CHS_MAX_ADDRESSABLE = (2**10) * (2**8 - 1) * (2**6 - 1) - 1

# CHS addressing using a custom disk geometry is not supported. Instead, only the
# following hard-coded geometry -- which corresponds to the typical logical disk
# geometry exposed by the majority of disk drives nowadays -- is used to perform CHS
# addressing. Values differing from the ones defined below are usually only found
# with quite old disk drives, anyway.
HEADS = 255
SECTORS_PER_TRACK = 63


def _lba_to_chs(lba: int, heads: int, sectors_per_track: int) -> tuple[int, int, int]:
    """Convert a logical block address to its cylinder-head-sector equivalent for a
    specific disk geometry.

    Returns a ``tuple`` of (cylinder, head, sector).
    """
    if lba < 0:
        raise ValueError('LBA must be zero or positive')
    if heads < 1:
        raise ValueError('Only positive values allowed for head')
    if sectors_per_track < 1:
        raise ValueError('Only positive values allowed for sectors_per_track')

    # heads == tracks per cylinder
    cylinder, rem = divmod(lba, sectors_per_track * heads)
    head = rem // sectors_per_track
    sector = rem % sectors_per_track + 1
    return cylinder, head, sector


def _pack_chs_address(
    cylinder: int, head: int, sector: int, *, check_validity: bool = True
) -> tuple[int, int, int]:
    """Get ``tuple`` of ``int`` representation of a cylinder-head-sector address as
    used in MBR partition entries.

    +---+---+---+---+---+---+---+---+
    |            head 7-0           |  byte 1
    +-------+-----------------------+
    | c 9-8 |        sec 5-0        |  byte 2
    +-------+-----------------------+
    |            cyl 7-0            |  byte 3
    +---+---+---+---+---+---+---+---+

    If ``check_validity`` is not set, this function only checks the validity of
    passed values in terms of whether they are technically able to be packed into the
    three-byte structure depicted above. Note that the following CHS addresses are
    also considered invalid:

    - All addresses with a sector value of 0
    - All addresses with a head value of 255 -- especially (1023, 255, 63)
    - (1023, 254, 63)

    Returns a ``tuple`` of (byte 1, byte 2, byte 3), each expressed as an ``int``.

    See https://en.wikipedia.org/wiki/Master_boot_record#Partition_table_entries.
    """
    if cylinder < 0 or head < 0 or sector < 0:
        raise OverflowError('Only positive values and zero allowed')
    if cylinder >= 1 << 10:
        raise OverflowError(
            f'Invalid cylinder value {cylinder}, must be a 10-bit value'
        )
    if head >= 1 << 8:
        raise OverflowError(f'Invalid head value {head}, must be an 8-bit value')
    if sector >= 1 << 6:
        raise OverflowError(f'Invalid sector value {sector}, must be a 6-bit value')

    if check_validity:
        if head == HEAD_INVALID:
            raise ValueError(f'Invalid head value, must not be {HEAD_INVALID}')
        if sector == SECTOR_INVALID:
            raise ValueError(f'Invalid sector value, must not be {SECTOR_INVALID}')
        address = (cylinder, head, sector)
        if address == CHS_INVALID:
            raise ValueError(f'Invalid address, must not be {CHS_INVALID}')

    byte_1 = head
    # cylinder mask: 0x300 == 0b1100000000
    byte_2_cyl_part = (cylinder & 0x300) >> 2
    byte_2 = byte_2_cyl_part | sector
    # cylinder mask: 0xFF == 0b0011111111
    byte_3 = cylinder & 0xFF

    return byte_1, byte_2, byte_3


def _check_lss(lss: int) -> None:
    """Check if a logical sector size of ``lss`` works with MBR partitioning."""
    if lss < MIN_LSS:
        raise ValueError(
            f'MBR partitioning requires a logical sector size of at least '
            f'{MIN_LSS} bytes'
        )


class PartitionType(IntEnum):
    """Common MBR partition type."""

    EMPTY = 0x00
    FAT12 = 0x01
    XENIX_ROOT = 0x02
    XENIX_USR = 0x03
    FAT16 = 0x04
    EXTENDED_CHS = 0x05
    FAT16B = 0x06
    NTFS = 0x07
    COMMODORE_FAT = 0x08
    FAT32_CHS = 0x0B
    FAT32_LBA = 0x0C
    FAT16B_LBA = 0x0E
    EXTENDED_LBA = 0x0F
    LINUX = 0x83
    LINUX_EXTENDED = 0x85
    LINUX_LVM = 0x8E
    ISO9660 = 0x96
    MAC_OSX_UFS = 0xA8
    MAC_OSX_BOOT = 0xAB
    HFS = 0xAF
    SOLARIS8_BOOT = 0xBE
    GPT_PROTECTIVE = 0xEE
    EFI_SYSTEM = 0xEF
    VMWARE_VMFS = 0xFB
    VMWARE_SWAP = 0xFC


class PartitionEntry:
    """MBR partition entry.

    Do not use ``__init__`` directly. Use ``PartitionEntry.new()``,
    ``PartitionEntry.new_empty()`` or ``PartitionEntry.from_bytes()`` instead.
    """

    SIZE = 16
    FORMAT = '<BBBBBBBBII'

    def __init__(self, start_lba: int, length_lba: int, type_: int, bootable: bool):
        self._start_lba = start_lba
        self._length_lba = length_lba
        self._type = type_
        self._bootable = bootable

    @classmethod
    def new(
        cls,
        start_lba: int,
        length_lba: int,
        type_: PartitionType | int,
        *,
        bootable: bool = False,
    ) -> 'PartitionEntry':
        """New non-empty partition entry."""
        if type_ == PartitionType.EMPTY:
            return cls.new_empty()

        byte_max = 1 << 8
        four_byte_max = 1 << 32

        if not 0 <= type_ < byte_max:
            raise ValueError(f'Invalid partition type {type_}, must be a 1-byte value')

        # LBA 0 is invalid because the partition table resides at LBA 0
        if not 0 < start_lba < four_byte_max:
            raise ValueError(
                f'Invalid partition starting sector {start_lba}, must be a 4-byte '
                f'value greater than 0'
            )
        if not 0 < length_lba < four_byte_max:
            raise ValueError(
                f'Invalid partition length {length_lba} sectors, must be a 4-byte '
                f'value greater than 0'
            )
        return cls(start_lba, length_lba, type_, bootable)

    @classmethod
    def new_empty(cls) -> 'PartitionEntry':
        """New empty / unused partition entry."""
        return cls(0, 0, PartitionType.EMPTY, False)

    @classmethod
    def from_bytes(cls, b: bytes) -> 'PartitionEntry':
        """Parse partition entry from ``bytes``.

        CHS addresses are ignored.
        """
        if len(b) != cls.SIZE:
            raise ValueError(
                f'MBR partition entry must be {cls.SIZE} bytes long, got {len(b)} bytes'
            )
        status, _, _, _, type_, _, _, _, start_lba, length_lba = struct.unpack(
            cls.FORMAT, b
        )

        # check if entry can be ignored
        if type_ == PartitionType.EMPTY or length_lba == 0:
            return cls.new_empty()

        if start_lba == 0:
            raise ParseError('Starting sector of partition must not be 0')

        bootable = bool(status & STATUS_ACTIVE)  # only check bit 7
        return cls(start_lba, length_lba, type_, bootable)

    def __bytes__(self) -> bytes:
        """Get ``bytes`` representation of partition entry."""
        if self.empty:
            return b'\x00' * self.SIZE

        status = STATUS_ACTIVE if self._bootable else STATUS_INACTIVE
        start_lba = self._start_lba
        end_lba = self._start_lba + self._length_lba - 1

        # only include each CHS address if it's unambiguous
        if start_lba > CHS_MAX_ADDRESSABLE:
            start_chs_packed = _pack_chs_address(*CHS_OVERFLOW, check_validity=False)
        else:
            start_chs = _lba_to_chs(start_lba, HEADS, SECTORS_PER_TRACK)
            start_chs_packed = _pack_chs_address(*start_chs)

        if end_lba > CHS_MAX_ADDRESSABLE:
            end_chs_packed = _pack_chs_address(*CHS_OVERFLOW, check_validity=False)
        else:
            end_chs = _lba_to_chs(end_lba, HEADS, SECTORS_PER_TRACK)
            end_chs_packed = _pack_chs_address(*end_chs)

        return struct.pack(
            self.FORMAT,
            status,
            *start_chs_packed,
            self._type,
            *end_chs_packed,
            self._start_lba,
            self._length_lba,
        )

    @property
    def start_lba(self) -> int:
        return self._start_lba

    @property
    def length_lba(self) -> int:
        return self._length_lba

    @property
    def end_lba(self) -> int:
        return self._start_lba + self._length_lba - 1

    @property
    def type(self) -> int:
        return self._type

    @property
    def empty(self) -> bool:
        return self._type == PartitionType.EMPTY

    @property
    def bootable(self) -> bool:
        return self._bootable


class Table:
    """MBR partition table.

    Do not use ``__init__`` directly, use ``Table.new()`` or ``Table.from_bytes()``
    instead.
    """

    SIZE = 512

    def __init__(self, partitions: tuple[PartitionEntry, ...], boot_code: bytes):
        check_overlapping(partitions, warn=True)
        self._partitions = partitions
        self._boot_code = boot_code

    @classmethod
    def new(
        cls, partitions: tuple[PartitionEntry, ...], *, boot_code: bytes = b''
    ) -> 'Table':
        """New partition table."""
        if len(partitions) > PARTITION_ENTRIES_COUNT:
            raise ValueError(
                f'Can only create a maximum of {PARTITION_ENTRIES_COUNT} partitions, '
                f'got {len(partitions)} partition entries'
            )
        if len(boot_code) > BOOT_CODE_SIZE:
            raise ValueError(
                f'MBR boot code can be at most {BOOT_CODE_SIZE} bytes long, got '
                f'{len(boot_code)} bytes'
            )
        # strip empty partition entries
        stripped_entries = tuple(filter(lambda p: not p.empty, partitions))
        return cls(stripped_entries, boot_code)

    @classmethod
    def from_bytes(cls, b: bytes) -> 'Table':
        """Parse partition table from ``bytes``."""
        if len(b) != cls.SIZE:
            raise ValueError(
                f'MBR partition table must be {cls.SIZE} bytes long, got {len(b)} bytes'
            )

        signature = b[SIGNATURE_START:]
        if signature != SIGNATURE:
            raise ParseError(f'Invalid MBR signature {signature!r}')

        partitions: list[PartitionEntry] = []
        for i in range(PARTITION_ENTRIES_COUNT):
            start = PARTITION_ENTRIES_START + PartitionEntry.SIZE * i
            end = start + PartitionEntry.SIZE
            entry_bytes = b[start:end]
            entry = PartitionEntry.from_bytes(entry_bytes)
            if not entry.empty:
                partitions.append(entry)

        boot_code = b[:BOOT_CODE_SIZE]
        return cls(tuple(partitions), boot_code)

    @classmethod
    def from_disk(cls, disk: 'Disk') -> 'Table':
        """Parse partition table from ``disk``."""
        if disk.sector_size.logical < MIN_LSS:
            raise ValueError(
                f'MBR partitioning requires a logical sector size of at least '
                f'{MIN_LSS} bytes'
            )
        first_sector = disk.read_at(0, 1)
        table_bytes = first_sector[: cls.SIZE]
        table = cls.from_bytes(table_bytes)

        # checks
        first_usable, last_usable = table.usable_lba(disk.size, disk.sector_size)
        for partition in table.partitions:
            check_bounds(partition, first_usable, last_usable, warn=True)
            check_alignment(partition, disk.sector_size, warn=True)

        return table

    def __bytes__(self) -> bytes:
        """Get ``bytes`` representation of MBR partition table."""
        # only warn to allow for hybrid MBRs
        check_overlapping(self._partitions, warn=True)

        # fill up with empty partition entries
        empty_entries_count = PARTITION_ENTRIES_COUNT - len(self._partitions)
        empty_entries = [PartitionEntry.new_empty() for _ in range(empty_entries_count)]
        entries = self._partitions + tuple(empty_entries)
        entries_bytes = b''.join(bytes(entry) for entry in entries)

        # fill up with zeroes
        boot_code_zeroes = b'\x00' * (BOOT_CODE_SIZE - len(self._boot_code))
        boot_code = self._boot_code + boot_code_zeroes

        b = boot_code + entries_bytes + SIGNATURE
        if len(b) != self.SIZE:
            raise RuntimeError(
                f'Failed to convert MBR partition table to bytes: Expected '
                f'{self.SIZE} bytes, got {len(b)} bytes'
            )
        return b

    def _write_to_disk(self, disk: 'Disk') -> None:
        """Write partition table to ``disk``."""
        _check_lss(disk.sector_size.logical)
        check_overlapping(self._partitions)
        first_usable, last_usable = self.usable_lba(disk.size, disk.sector_size)

        for partition in self._partitions:
            check_bounds(partition, first_usable, last_usable)
            check_alignment(partition, disk.sector_size, warn=True)

        disk.write_at(0, bytes(self), fill_zeroes=True)

    # noinspection PyMethodMayBeStatic
    def usable_lba(self, disk_size: int, sector_size: SectorSize) -> tuple[int, int]:
        """Return a ``tuple`` of the first and last logical sector which may be used
        by a partition described by a partition entry of this partition table.
        """
        lss = sector_size.logical
        _check_lss(lss)
        last_usable = disk_size // sector_size.logical - 1
        return 1, last_usable

    @property
    def type(self) -> TableType:
        return TableType.MBR

    @property
    def partitions(self) -> tuple[PartitionEntry, ...]:
        return self._partitions

    @property
    def boot_code(self) -> bytes:
        return self._boot_code
