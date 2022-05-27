"""GPT partitioning.

See https://uefi.org/specifications.
"""

import logging
import struct
from enum import Enum, Flag
from typing import TYPE_CHECKING, Any, Iterable, Optional
from uuid import UUID, uuid4
from zlib import crc32

from .._base import ParseError, SectorSize
from . import mbr
from ._base import TableType, check_alignment, check_bounds, check_overlapping

if TYPE_CHECKING:
    from ..disk import Disk

__all__ = ['Table', 'PartitionEntry', 'PartitionAttributes', 'PartitionType']


log = logging.getLogger(__name__)


MIN_LSS = 512  # minimum logical sector size required for GPT partitioning

PRIMARY_HEADER_LBA = 1

SIGNATURE = b'EFI PART'
REVISION = 0x00010000  # 1.0
MIN_PARTITION_ENTRIES = 128

PARTITION_NAME_MAX_LEN = 36  # 36 characters, 72 bytes with encoding UTF-16LE


def _is_power_of_two(value: int) -> bool:
    """Check if ``value`` is a power of two.

    ``value`` must be an ``int`` greater than zero.

    Returns whether ``value`` can be expressed as 2 to the power of x, while x is an
    integer greater than or equal to zero.
    """
    if value <= 0:
        raise ValueError('Value must be greater than 0')
    return value & (value - 1) == 0


def _check_lss(lss: int) -> None:
    """Check if a logical sector size of ``lss`` works with GPT partitioning."""
    if lss < MIN_LSS:
        raise ValueError(
            f'GPT partitioning requires a logical sector size of at least '
            f'{MIN_LSS} bytes'
        )
    if not _is_power_of_two(lss):
        raise ValueError(
            'Logical sector size must be a power of 2 for GPT partitioning'
        )


def _partition_array_sectors(entries_count: int, entry_size: int, lss: int) -> int:
    """Return how many sectors a GPT partition array with ``entries_count``
    partitions and a partition entry size of ``entry_size`` would occupy, given a
    logical sector size of ``lss``.

    This calculation is necessary because we always need to reserve whole sectors for
    partition entries.
    """
    if lss % entry_size != 0:
        raise ValueError(
            'Logical sector size must be a multiple of the partition entry size'
        )
    return (entries_count * entry_size - 1) // lss + 1


def _partition_entries_written(entries_count: int, entry_size: int, lss: int) -> int:
    """Return the amount of total partition entries of size ``entry_size`` to be
    written to the disk, given ``entries_count`` actually used partitions and a
    logical sector size of ``lss``.

    The total amount of partition entries written might be higher than the amount of
    partitions actually used because:

    - We always reserve whole sectors for partition entries.
    - We always reserve space for at least 128 partition entries.
    """
    if lss % entry_size != 0:
        raise ValueError(
            'Logical sector size must be a multiple of the partition entry size'
        )
    sectors_written = _partition_array_sectors(entries_count, entry_size, lss)
    entries_per_sector = lss // entry_size
    entries_written = sectors_written * entries_per_sector
    return max(entries_written, MIN_PARTITION_ENTRIES)


class PartitionType(Enum):
    """Common GPT partition type."""

    UNUSED = UUID('00000000-0000-0000-0000-000000000000')
    MBR_PARTITION_SCHEME = UUID('024DEE41-33E7-11D3-9D69-0008C781F39F')
    EFI_SYSTEM_PARTITION = UUID('C12A7328-F81F-11D2-BA4B-00A0C93EC93B')
    GRUB_BIOS_BOOT = UUID('21686148-6449-6E6F-744E-656564454649')

    MICROSOFT_BASIC_DATA = UUID('EBD0A0A2-B9E5-4433-87C0-68B6B72699C7')
    MICROSOFT_LDM_DATA = UUID('AF9B60A0-1431-4F62-BC68-3311714A69AD')
    MICROSOFT_LDM_METADATA = UUID('5808C8AA-7E8F-42E0-85D2-E1E90434CFB3')
    MICROSOFT_MSFT_RECOVERY = UUID('DE94BBA4-06D1-4D40-A16A-BFD50179D6AC')
    MICROSOFT_MSFT_RESERVED = UUID('E3C9E316-0B5C-4DB8-817D-F92DF00215AE')

    LINUX_FILESYSTEM = UUID('0FC63DAF-8483-4772-8E79-3D69D8477DE4')
    LINUX_RAID = UUID('A19D880F-05FC-4D3B-A006-743F0F84911E')
    LINUX_ROOT_X86 = UUID('44479540-F297-41B2-9AF7-D131D5F0458A')
    LINUX_ROOT_X86_64 = UUID('4F68BCE3-E8CD-4DB1-96E7-FBCAF984B709')
    LINUX_ROOT_ARM32 = UUID('69DAD710-2CE4-4E3C-B16C-21A1D49ABED3')
    LINUX_ROOT_ARM64 = UUID('B921B045-1DF0-41C3-AF44-4C6F280D3FAE')
    LINUX_BOOT = UUID('BC13C2FF-59E6-4262-A352-B275FD6F7172')
    LINUX_HOME = UUID('933AC7E1-2EB4-4F13-B844-0E14E2AEF915')
    LINUX_SRV = UUID('3B8F8425-20E0-4F3B-907F-1A25A76F98E8')
    LINUX_SWAP = UUID('0657FD6D-A4AB-43C4-84E5-0933C84B4F4F')
    LINUX_LVM = UUID('E6D6D379-F507-44C2-A23C-238F2A3DF928')
    LINUX_DMCRYPT = UUID('7FFEC5C9-2D00-49B7-8941-3EA10A5586B7')
    LINUX_LUKS = UUID('CA7D7CCB-63ED-4C53-861C-1742536059CC')

    FREEBSD_BOOT = UUID('83BD6B9D-7F41-11DC-BE0B-001560B84F0F')
    FREEBSD_DISKLABEL = UUID('516E7CB4-6ECF-11D6-8FF8-00022D09712B')
    FREEBSD_SWAP = UUID('516E7CB5-6ECF-11D6-8FF8-00022D09712B')
    FREEBSD_UFS = UUID('516E7CB6-6ECF-11D6-8FF8-00022D09712B')

    VMWARE_VMFS = UUID('AA31E02A-400F-11DB-9590-000C2911D1B8')


class PartitionAttributes(Flag):
    """GPT partition attribute flags.

    - Bits 0-2 are defined for all partition types.
    - Bits 3-47 are reserved for future use.
    - Bits 48-63 are defined and used by the individual partition type.
    """

    REQUIRED = 1 << 0  # required for the platform to function
    EFI_IGNORE = 1 << 1  # file system mappings will not be created
    BIOS_BOOTABLE = 1 << 2  # equivalent of MBR active flag


class PartitionEntry:
    """GPT partition entry.

    Do not use ``__init__`` directly, use ``PartitionEntry.new()`` or
    ``PartitionEntry.new_empty()`` instead.
    """

    SIZE = 128
    FORMAT = '<16s16sQQQ72s'

    def __init__(
        self,
        start_lba: int,
        end_lba: int,
        type_: UUID,
        attributes: int,
        guid: UUID,
        name: str,
    ):
        self._start_lba = start_lba
        self._end_lba = end_lba
        self._type = type_
        self._attributes = attributes
        self._guid = guid
        self._name = name

    @classmethod
    def new(
        cls,
        start_lba: int,
        length_lba: int,
        type_: PartitionType | UUID,
        *,
        attributes: PartitionAttributes | int = 0,
        guid: UUID = None,
        name: str = '',
    ) -> 'PartitionEntry':
        """New non-empty partition entry.

        ``PartitionType.UNUSED`` must not be passed as ``type_``, use
        ``PartitionEntry.new_empty()`` instead.
        """
        if isinstance(type_, PartitionType):
            type_uuid = type_.value
        else:
            type_uuid = type_

        if type_uuid == PartitionType.UNUSED.value:
            raise ValueError(
                'Use PartitionEntry.new_empty() to create an empty partition entry'
            )

        if isinstance(attributes, PartitionAttributes):
            attributes_int = attributes.value
        else:
            attributes_int = attributes

        eight_byte_max = 1 << 64
        end_lba = start_lba + length_lba - 1

        if length_lba <= 0:
            raise ValueError(
                f'Invalid partition length {length_lba} sectors, must be greater than 0'
            )
        if not 2 < start_lba < eight_byte_max:
            raise ValueError(
                f'Invalid partition starting sector {start_lba}, must be an 8-byte '
                f'value greater than 2'
            )
        if not 2 < end_lba < eight_byte_max:
            raise ValueError(
                f'Invalid partition ending sector {end_lba}, must be an 8-byte value '
                f'value greater than 2'
            )
        if not 0 <= attributes_int < eight_byte_max:
            raise ValueError(
                f'Invalid partition attributes {hex(attributes_int)}, must be an '
                f'8-byte value'
            )
        if len(name) > PARTITION_NAME_MAX_LEN:
            raise ValueError(
                f'Partition name must not be longer than {PARTITION_NAME_MAX_LEN} '
                f'characters, got {name!r}'
            )

        if guid is None:
            guid = uuid4()
        name = name.rstrip('\x00')

        return cls(start_lba, end_lba, type_uuid, attributes_int, guid, name)

    @classmethod
    def new_empty(cls) -> 'PartitionEntry':
        """New empty / unused partition entry."""
        return cls(0, 0, PartitionType.UNUSED.value, 0, uuid4(), '')

    @classmethod
    def from_bytes(cls, b: bytes) -> 'PartitionEntry':
        """Parse partition entry from ``bytes``."""
        # Earlier specifications allowed a partition entry to be any multiple of
        # 8 bytes long. As per newer specifications, we only allow powers of two
        # >= 128 bytes as partition entry lengths.
        if len(b) < cls.SIZE:
            raise ValueError(
                f'GPT partition entry must be a minimum of {cls.SIZE} bytes long, '
                f'got {len(b)} bytes'
            )
        if not _is_power_of_two(len(b)):
            raise ValueError(
                f'GPT partition entry size must be a power of 2, got {len(b)} bytes'
            )

        (
            type_bytes,
            guid_bytes,
            start_lba,
            end_lba,
            attributes,
            name_bytes,
        ) = struct.unpack(cls.FORMAT, b[: cls.SIZE])

        type_ = UUID(bytes_le=type_bytes)

        # check if entry can be ignored
        if type_ == PartitionType.UNUSED.value:
            return cls.new_empty()

        if start_lba <= 2:
            raise ParseError('Starting sector of partition must be greater than 2')

        if start_lba > end_lba:
            raise ParseError(
                f'Starting sector of partition must be greater or equal to the ending '
                f'sector (got starting sector {start_lba}, ending sector {end_lba})'
            )

        guid = UUID(bytes_le=guid_bytes)
        name = name_bytes.decode('utf-16le').rstrip('\x00')
        return cls(start_lba, end_lba, type_, attributes, guid, name)

    def __bytes__(self) -> bytes:
        """Get ``bytes`` representation of partition entry."""
        if self.empty:
            return b'\x00' * self.SIZE

        return struct.pack(
            self.FORMAT,
            self._type.bytes_le,
            self._guid.bytes_le,
            self._start_lba,
            self._end_lba,
            self._attributes,
            self.name.encode('utf-16le'),
        )

    @property
    def start_lba(self) -> int:
        return self._start_lba

    @property
    def end_lba(self) -> int:
        return self._end_lba

    @property
    def length_lba(self) -> int:
        return self._end_lba - self._start_lba + 1

    @property
    def type(self) -> UUID:
        return self._type

    @property
    def empty(self) -> bool:
        return self._type == PartitionType.UNUSED.value

    @property
    def attributes(self) -> int:
        return self._attributes

    @property
    def guid(self) -> UUID:
        return self._guid

    @property
    def name(self) -> str:
        return self._name

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, PartitionEntry):
            return (
                self._start_lba == other._start_lba
                and self._end_lba == other._end_lba
                and self._type == other._type
                and self._attributes == other._attributes
                and self._guid == other._guid
                and self._name == other._name
            )
        return NotImplemented

    def __repr__(self) -> str:
        return (
            f'gpt.{self.__class__.__name__}(start_lba={self._start_lba}, '
            f'end_lba={self._end_lba}, type={self._type!r}, '
            f'attributes={hex(self._attributes)}, guid={self._guid!r}, '
            f'name={self._name!r})'
        )


class Table:
    """GUID partition table.

    Do not use ``__init__`` directly, use ``Table.new()`` instead.
    """

    HEADER_SIZE = 92
    HEADER_FORMAT = '<8sIIIIQQQQ16sQIII'

    def __init__(
        self,
        partitions: Iterable[PartitionEntry],
        disk_guid: UUID,
        custom_mbr: Optional[mbr.Table],
    ):
        partitions = tuple(partitions)
        check_overlapping(partitions, warn=True)
        self._partitions = partitions
        self._disk_guid = disk_guid
        self._custom_mbr = custom_mbr

    @classmethod
    def new(
        cls,
        partitions: Iterable[PartitionEntry],
        *,
        disk_guid: UUID = None,
        custom_mbr: mbr.Table = None,
    ) -> 'Table':
        """New partition table."""
        # strip empty partition entries
        stripped_entries = filter(lambda p: not p.empty, partitions)

        if disk_guid is None:
            disk_guid = uuid4()
        return cls(stripped_entries, disk_guid, custom_mbr)

    @classmethod
    def _validate_header(
        cls,
        header_sector: bytes,
        expected_header_lba: int,
        expected_alternate_header_lba: int,
    ) -> None:
        lss = len(header_sector)

        (
            signature,
            revision,
            header_size,
            header_crc32,
            _,  # reserved
            header_lba,
            alternate_header_lba,
            first_usable_lba,
            last_usable_lba,
            _,  # disk GUID
            partition_array_lba,
            partition_entries_count,
            partition_entry_size,
            _,  # partition entry array CRC32
        ) = struct.unpack(cls.HEADER_FORMAT, header_sector[: cls.HEADER_SIZE])

        if signature != SIGNATURE:
            raise ParseError(f'Invalid GPT signature {signature!r}')

        if revision != REVISION:
            raise ParseError(f'Invalid GPT header revision number {revision}')

        if not cls.HEADER_SIZE <= header_size <= lss:
            raise ParseError(
                f'Header size specified in GPT header must be in range '
                f'({cls.HEADER_SIZE}, {lss}), got {header_size}'
            )

        header = header_sector[:header_size]
        header_for_crc32 = header[:16] + b'\x00' * 4 + header[20:]

        if crc32(header_for_crc32) != header_crc32:
            raise ParseError('CRC32 of GPT header does not match')

        if header_lba != expected_header_lba:
            raise ParseError(
                f'Header sector does not match (expected LBA {expected_header_lba}, '
                f'got LBA {header_lba})'
            )

        if alternate_header_lba != expected_alternate_header_lba:
            raise ParseError(
                f'Alternate header sector does not match (expected LBA '
                f'{expected_alternate_header_lba}, got LBA {alternate_header_lba})'
            )

        if partition_entry_size < PartitionEntry.SIZE:
            raise ParseError(
                f'GPT partition entry size must be a minimum of {PartitionEntry.SIZE} '
                f'bytes, got {partition_entry_size} bytes'
            )
        if not _is_power_of_two(partition_entry_size):
            raise ParseError(
                f'GPT partition entry size must be a power of 2, got '
                f'{partition_entry_size} bytes'
            )

        if partition_entries_count < MIN_PARTITION_ENTRIES:
            raise ParseError(
                f'GPT partition entry array must hold a minimum of '
                f'{MIN_PARTITION_ENTRIES} partition entries, got '
                f'{partition_entries_count}'
            )

        partition_array_sectors = _partition_array_sectors(
            partition_entries_count, partition_entry_size, lss
        )
        partition_array_end_lba = partition_array_lba + partition_array_sectors - 1

        if header_lba < alternate_header_lba:
            # primary GPT
            alt_partition_array_lba = alternate_header_lba - partition_array_sectors
            alt_partition_array_end_lba = alternate_header_lba - 1
            if not (
                header_lba
                < partition_array_lba
                < partition_array_end_lba
                < first_usable_lba
                <= last_usable_lba
                < alt_partition_array_lba
                < alt_partition_array_end_lba
                < alternate_header_lba
            ):
                raise ParseError(
                    'Invalid combination of logical block addresses found in primary '
                    'GPT header'
                )
        else:
            # backup GPT
            alt_partition_array_lba = alternate_header_lba + 1
            alt_partition_array_end_lba = alternate_header_lba + partition_array_sectors
            if not (
                alternate_header_lba
                < alt_partition_array_lba
                < alt_partition_array_end_lba
                < first_usable_lba
                <= last_usable_lba
                < partition_array_lba
                < partition_array_end_lba
                < header_lba
            ):
                raise ParseError(
                    'Invalid combination of logical block addresses found in '
                    'secondary GPT header'
                )

    @classmethod
    def _validate_partition_array(
        cls, header_sector: bytes, partition_array: bytes
    ) -> None:

        _h = struct.unpack(cls.HEADER_FORMAT, header_sector[: cls.HEADER_SIZE])
        _, _, _, _, _, _, _, _, _, _, _, entries_count, entry_size, array_crc32 = _h

        expected_array_size = entries_count * entry_size
        if len(partition_array) != expected_array_size:
            raise ValueError(
                f'Calculated partition array size does not match passed partition '
                f'array (expected {expected_array_size} bytes, got '
                f'{len(partition_array)} bytes)'
            )

        if crc32(partition_array) != array_crc32:
            raise ParseError('CRC32 of partition entry array does not match')

    @classmethod
    def from_disk(cls, disk: 'Disk') -> 'Table':
        """Parse partition table from ``disk``."""
        lss = disk.sector_size.logical
        _check_lss(lss)

        def get_partition_array(header_bytes: bytes) -> bytes:
            """Get ``bytes`` of the partition entry array pointed to by a GPT header.

            :param header_bytes: Bytes of an already validated GPT header.
            """
            h = struct.unpack(cls.HEADER_FORMAT, header_bytes[: cls.HEADER_SIZE])
            _, _, _, _, _, _, _, _, _, _, array_lba, entries_count_, entry_size_, _ = h

            # last sector might not be fully filled with partition entries
            array_sectors = _partition_array_sectors(entries_count_, entry_size_, lss)
            array_bytes = entries_count_ * entry_size_
            return disk.read_at(array_lba, array_sectors)[:array_bytes]

        last_sector_lba = disk.size // lss - 1

        # first try to parse GPT header at LBA 1
        header_sector = disk.read_at(PRIMARY_HEADER_LBA, 1)

        try:
            # always expecting backup table at last sector
            cls._validate_header(header_sector, PRIMARY_HEADER_LBA, last_sector_lba)
            partition_array = get_partition_array(header_sector)
            cls._validate_partition_array(header_sector, partition_array)

        except ParseError as e:
            # parsing of primary table failed, try backup table
            log.debug(f'Failed to parse primary GPT: {e}')
            header_sector = disk.read_at(last_sector_lba, 1)

            try:
                # always expecting primary table at first sector
                cls._validate_header(header_sector, last_sector_lba, PRIMARY_HEADER_LBA)
                partition_array = get_partition_array(header_sector)
                cls._validate_partition_array(header_sector, partition_array)

            except ParseError as e2:
                log.debug(f'Failed to parse secondary GPT: {e2}')
                raise ParseError('No valid GPT found')

        _h = struct.unpack(cls.HEADER_FORMAT, header_sector[: cls.HEADER_SIZE])
        _, _, _, _, _, _, _, _, _, disk_guid_bytes, _, entries_count, entry_size, _ = _h

        # parse partition array
        partitions: list[PartitionEntry] = []
        for i in range(entries_count):
            start = entry_size * i
            end = start + entry_size
            entry_bytes = partition_array[start:end]
            entry = PartitionEntry.from_bytes(entry_bytes)
            if not entry.empty:
                partitions.append(entry)

        # parse MBR
        mbr_ = mbr.Table.from_disk(disk)

        if (
            len(mbr_.partitions) == 1
            and mbr_.partitions[0].type == mbr.PartitionType.GPT_PROTECTIVE.value
        ):
            custom_mbr = None  # standard protective MBR
        else:
            custom_mbr = mbr_

        disk_guid = UUID(bytes_le=disk_guid_bytes)
        table = cls(partitions, disk_guid, custom_mbr)

        # checks
        first_usable, last_usable = table.usable_lba(disk.size, disk.sector_size)
        for partition in table.partitions:
            check_bounds(partition, first_usable, last_usable, warn=True)
            check_alignment(partition, disk.sector_size, warn=True)

        return table

    def bytes_tuple(
        self,
        disk_size: int,
        sector_size: SectorSize,
    ) -> tuple[bytes, bytes, bytes]:
        """Get ``bytes`` representation of partition table elements.

        Returns a tuple of (primary header, backup header, partition array), each in
        the form of a ``bytes`` object.
        """
        # checks
        lss = sector_size.logical
        _check_lss(lss)
        check_overlapping(self._partitions)
        first_usable, last_usable = self.usable_lba(disk_size, sector_size)

        for partition in self._partitions:
            check_bounds(partition, first_usable, last_usable)
            check_alignment(partition, sector_size, warn=True)

        # prepare partition entry array
        entries_count = _partition_entries_written(
            len(self._partitions), PartitionEntry.SIZE, sector_size.logical
        )
        empty_entries_count = entries_count - len(self._partitions)
        empty_entries = [PartitionEntry.new_empty() for _ in range(empty_entries_count)]
        entries = self._partitions + tuple(empty_entries)
        entry_array = b''.join(bytes(entry) for entry in entries)

        # prepare headers
        backup_header_lba = disk_size // lss - 1
        first_usable_lba, last_usable_lba = self.usable_lba(disk_size, sector_size)
        disk_guid_bytes = self._disk_guid.bytes_le

        primary_partition_array_lba = PRIMARY_HEADER_LBA + 1
        array_sectors = _partition_array_sectors(
            entries_count, PartitionEntry.SIZE, lss
        )
        backup_partition_array_lba = backup_header_lba - array_sectors

        primary_header_fields = [
            SIGNATURE,
            REVISION,
            self.HEADER_SIZE,
            0,  # placeholder for header CRC32 (!)
            0,  # reserved
            PRIMARY_HEADER_LBA,  # header LBA (!)
            backup_header_lba,  # alternate header LBA (!)
            first_usable_lba,
            last_usable_lba,
            disk_guid_bytes,
            primary_partition_array_lba,  # (!)
            entries_count,
            PartitionEntry.SIZE,
            crc32(entry_array),
        ]

        backup_header_fields = primary_header_fields[:]
        backup_header_fields[5] = backup_header_lba
        backup_header_fields[6] = PRIMARY_HEADER_LBA
        backup_header_fields[10] = backup_partition_array_lba

        primary_header = struct.pack(self.HEADER_FORMAT, *primary_header_fields)
        backup_header = struct.pack(self.HEADER_FORMAT, *backup_header_fields)

        # insert header CRC32
        ph_crc32 = crc32(primary_header).to_bytes(4, 'little')
        bh_crc32 = crc32(backup_header).to_bytes(4, 'little')
        primary_header = primary_header[:16] + ph_crc32 + primary_header[20:]
        backup_header = backup_header[:16] + bh_crc32 + backup_header[20:]

        return primary_header, backup_header, entry_array

    def _write_to_disk(self, disk: 'Disk') -> None:
        """Write partition table to ``disk``."""
        lss = disk.sector_size.logical
        disk_size_lba = disk.size // lss

        # checks are done in bytes_tuple()
        primary_header, backup_header, partition_array = self.bytes_tuple(
            disk.size, disk.sector_size
        )

        # prepare MBR
        if self._custom_mbr is not None:
            mbr_ = self._custom_mbr
        else:
            # generate protective MBR
            start_lba = 1
            length_lba = min(disk_size_lba - 1, 0xFFFFFFFF)
            mbr_ = mbr.Table.new(
                (
                    mbr.PartitionEntry.new(
                        start_lba, length_lba, mbr.PartitionType.GPT_PROTECTIVE
                    ),
                )
            )

        # write to disk
        last_sector_lba = disk.size // disk.sector_size.logical - 1
        partition_entries_written = _partition_entries_written(
            len(self._partitions), PartitionEntry.SIZE, lss
        )
        partition_array_sectors = _partition_array_sectors(
            partition_entries_written, PartitionEntry.SIZE, lss
        )
        backup_partition_array_lba = last_sector_lba - partition_array_sectors

        disk.write_at(0, bytes(mbr_), fill_zeroes=True)
        disk.write_at(PRIMARY_HEADER_LBA, primary_header, fill_zeroes=True)
        disk.write_at(PRIMARY_HEADER_LBA + 1, partition_array)
        disk.write_at(backup_partition_array_lba, partition_array)
        disk.write_at(last_sector_lba, backup_header, fill_zeroes=True)

    def usable_lba(self, disk_size: int, sector_size: SectorSize) -> tuple[int, int]:
        """Return a ``tuple`` of the first and last logical sector which may be used
        by a partition described by a partition entry of this partition table.
        """
        lss = sector_size.logical
        _check_lss(lss)
        last_sector = disk_size // sector_size.logical - 1

        entries = _partition_entries_written(
            len(self._partitions), PartitionEntry.SIZE, lss
        )
        array_sectors = _partition_array_sectors(entries, PartitionEntry.SIZE, lss)

        first_usable = PRIMARY_HEADER_LBA + array_sectors + 1
        last_usable = last_sector - array_sectors - 1
        return first_usable, last_usable

    @property
    def type(self) -> TableType:
        return TableType.GPT

    @property
    def partitions(self) -> tuple[PartitionEntry, ...]:
        return self._partitions

    @property
    def disk_guid(self) -> UUID:
        return self._disk_guid

    @property
    def custom_mbr(self) -> Optional[mbr.Table]:
        return self._custom_mbr

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Table):
            return (
                self._partitions == other._partitions
                and self._disk_guid == other._disk_guid
                and self._custom_mbr == other._custom_mbr
            )
        return NotImplemented

    def __repr__(self) -> str:
        return (
            f'gpt.{self.__class__.__name__}({len(self._partitions)}, '
            f'disk_guid={self._disk_guid!r})'
        )
