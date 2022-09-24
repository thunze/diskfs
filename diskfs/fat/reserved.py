"""Structures found in the reserved region of a FAT file system."""

from __future__ import annotations

import warnings
from dataclasses import dataclass

# noinspection PyUnresolvedReferences, PyProtectedMember
from typing import TYPE_CHECKING, ClassVar, Protocol, Type, _ProtocolMeta

from typing_extensions import Annotated

from ..base import ValidationError, ValidationWarning, is_power_of_two
from ..bytestruct import ByteStruct
from .base import FatType

if TYPE_CHECKING:
    from ..volume import Volume

__all__ = [
    'Bpb',
    'BpbDos200',
    'BpbDos331',
    'ShortEbpbFat',
    'ShortEbpbFat32',
    'EbpbFat',
    'EbpbFat32',
    'BootSectorStart',
    'BootSector',
    'FsInfo',
    'CLUSTER_SIZE_DEFAULT',
    'MEDIA_TYPE_DEFAULT',
    'SECTORS_PER_TRACK_DEFAULT',
    'HEADS_DEFAULT',
    'PHYSICAL_DRIVE_NUMBER_DEFAULT',
    'VOLUME_LABEL_DEFAULT',
    'BOOT_CODE_DUMMY',
]


MIN_LSS_FAT = 128
MIN_LSS_FAT32 = 512
SECTORS_PER_TRACK_MAX = 63
HEADS_MAX = 255
PHYSICAL_DRIVE_NUMBERS_RESERVED = (0x7F, 0xFF)
EXTENDED_BOOT_SIGNATURES = (b'\x28', b'\x29')
EXTENDED_BOOT_SIGNATURE_EXISTS = b'\x29'  # EBPB extension exists
FILE_SYSTEM_TYPES_FAT = (b'FAT12   ', b'FAT16   ', b'FAT     ')
FILE_SYSTEM_TYPE_FAT32 = b'FAT32   '
FAT32_VERSION = 0
SECTOR_NUMBERS_UNUSED = (0, 0xFFFF)
FS_INFO_SECTOR = 1

JUMP_INSTRUCTIONS_START = (b'\xEB', b'\xE9', b'\x90\xEB')
OEM_NAMES_COMMON = (
    b'MSDOS5.0',
    b'MSWIN4.1',
    b'IBM  3.3',
    b'IBM  7.1',
    b'mkdosfs ',
    b'FreeDOS ',
)
SIGNATURE = b'\x55\xaa'

# FS info sector
FS_INFO_SIGNATURE_1 = b'RRaA'
FS_INFO_SIGNATURE_2 = b'rrAa'
FS_INFO_SIGNATURE_3 = b'\x00\x00' + SIGNATURE
FS_INFO_UNKNOWN = 0xFFFFFFFF

# defaults
CLUSTER_SIZE_DEFAULT = 16
ROOTDIR_ENTRIES_DEFAULT = 240
MEDIA_TYPE_DEFAULT = 0xF8
SECTORS_PER_TRACK_DEFAULT = 63
HEADS_DEFAULT = 255
PHYSICAL_DRIVE_NUMBER_DEFAULT = 0x80
VOLUME_LABEL_DEFAULT = b'NO NAME    '
BOOT_CODE_DUMMY = b'\xF4\xEB\xFD'  # endless loop

# other constants
DIRECTORY_ENTRY_SIZE = 32


class _BpbMeta(_ProtocolMeta):

    # noinspection PyMethodParameters
    def __len__(cls) -> int:
        ...


# noinspection PyPropertyDefinition
class Bpb(Protocol, metaclass=_BpbMeta):
    """BIOS parameter block."""

    def validate_for_volume(self, volume: 'Volume', *, recurse: bool = False) -> None:
        ...

    @classmethod
    def from_bytes(cls, b: bytes) -> Bpb:
        ...

    def __bytes__(self) -> bytes:
        ...

    def __len__(self) -> int:
        ...

    @property
    def bpb_dos_200(self) -> BpbDos200:
        ...

    @property
    def total_size(self) -> int | None:
        ...

    @property
    def fat_size(self) -> int:
        ...


@dataclass(frozen=True)
class BpbDos200(ByteStruct):
    """DOS 2.0 BIOS parameter block."""

    lss: Annotated[int, 2]
    cluster_size: Annotated[int, 1]
    reserved_size: Annotated[int, 2]
    fat_count: Annotated[int, 1]
    rootdir_entries: Annotated[int, 2]  # max value, some entries might be zeroed out
    total_size_200: Annotated[int, 2]
    media_type: Annotated[int, 1]
    fat_size_200: Annotated[int, 2]

    def validate(self) -> None:
        if self.lss < DIRECTORY_ENTRY_SIZE:
            raise ValidationError(
                f'Logical sector size must be greater than or equal to '
                f'{DIRECTORY_ENTRY_SIZE}'
            )
        if not is_power_of_two(self.lss):
            raise ValidationError('Logical sector size must be a power of 2')
        if self.cluster_size <= 0:
            raise ValidationError('Cluster size must be greater than 0')
        if not is_power_of_two(self.cluster_size):
            raise ValidationError('Cluster size must be a power of 2')
        if self.reserved_size < 1:
            raise ValidationError('Reserved sector count must be greater than 0')
        if self.fat_count < 1:
            raise ValidationError('FAT count must be greater than 0')
        if self.media_type <= 0xEF or (0xF1 <= self.media_type <= 0xF7):
            raise ValidationError(f'Unsupported value media type {self.media_type}')

    def validate_for_volume(self, volume: Volume, *, recurse: bool = False) -> None:
        super().validate_for_volume(volume, recurse=recurse)
        lss = volume.sector_size.logical

        if self.lss != lss:
            raise ValidationError(
                'Logical sector size in DOS 2.0 BPB does not match logical sector '
                'size of disk'
            )
        if (self.rootdir_entries * DIRECTORY_ENTRY_SIZE) % lss != 0:
            raise ValidationError(
                'Root directory entries must align with logical sector size of disk'
            )
        if self.total_size_200 > volume.size_lba:
            raise ValidationError('Total size must not be greater than volume size')

    @property
    def bpb_dos_200(self) -> BpbDos200:
        return self

    @property
    def total_size(self) -> int | None:
        return self.total_size_200 or None

    @property
    def fat_size(self) -> int:
        return self.fat_size_200


@dataclass(frozen=True)
class BpbDos331(ByteStruct):
    """DOS 3.31 BIOS parameter block."""

    bpb_dos_200_: BpbDos200
    sectors_per_track: Annotated[int, 2]
    heads: Annotated[int, 2]
    hidden_before_partition: Annotated[int, 4]
    total_size_331: Annotated[int, 4]

    def validate(self) -> None:
        if self.sectors_per_track > SECTORS_PER_TRACK_MAX:
            raise ValidationError(
                f'Sector count per track must be a maximum of {SECTORS_PER_TRACK_MAX}'
            )
        if self.heads > HEADS_MAX:
            raise ValidationError(f'Head count must be a maximum of {HEADS_MAX}')

        # total sizes must match if none of them is 0
        total_size_200 = self.bpb_dos_200_.total_size
        total_size_331 = self.total_size_331
        if total_size_200 and total_size_331 and total_size_200 != total_size_331:
            raise ValidationError(
                'Total size does not match total size defined in DOS 2.0 BPB'
            )

    def validate_for_volume(self, volume: Volume, *, recurse: bool = False) -> None:
        super().validate_for_volume(volume, recurse=recurse)
        if self.hidden_before_partition != volume.start_lba:
            raise ValidationError(
                'Hidden sector count does not match volume start sector'
            )
        if self.total_size_331 > volume.size_lba:
            raise ValidationError('Total size must not be greater than volume size')

    @property
    def bpb_dos_200(self) -> BpbDos200:
        return self.bpb_dos_200_

    @property
    def total_size(self) -> int | None:
        return self.bpb_dos_200_.total_size or self.total_size_331 or None

    @property
    def fat_size(self) -> int:
        return self.bpb_dos_200_.fat_size


def _check_physical_drive_number(physical_drive_number: int) -> None:
    if physical_drive_number in PHYSICAL_DRIVE_NUMBERS_RESERVED:
        warnings.warn(
            f'Reserved physical drive number {physical_drive_number}', ValidationWarning
        )


def _check_extended_boot_signature(extended_boot_signature: bytes) -> None:
    if extended_boot_signature not in EXTENDED_BOOT_SIGNATURES:
        raise ValidationError(
            f'Invalid extended boot signature {extended_boot_signature!r}'
        )


@dataclass(frozen=True)
class ShortEbpbFat(ByteStruct):

    bpb_dos_331: BpbDos331
    physical_drive_number: Annotated[int, 1]
    reserved: Annotated[int, 1]
    extended_boot_signature: Annotated[bytes, 1]

    def validate(self) -> None:
        # previous BPBs
        if self.bpb_dos_331.bpb_dos_200.lss < MIN_LSS_FAT:
            raise ValidationError(
                f'FAT requires a logical sector size of at least {MIN_LSS_FAT} bytes'
            )
        if self.bpb_dos_331.bpb_dos_200.rootdir_entries <= 0:
            raise ValidationError('Root directory entry count must be greater than 0')
        if self.bpb_dos_331.bpb_dos_200.fat_size <= 0:
            raise ValidationError(
                'FAT size defined in DOS 2.0 BPB must be greater than 0'
            )

        # this EBPB
        _check_physical_drive_number(self.physical_drive_number)
        _check_extended_boot_signature(self.extended_boot_signature)

    @property
    def bpb_dos_200(self) -> BpbDos200:
        return self.bpb_dos_331.bpb_dos_200

    @property
    def total_size(self) -> int | None:
        return self.bpb_dos_331.total_size

    @property
    def fat_size(self) -> int:
        return self.bpb_dos_331.fat_size


@dataclass(frozen=True)
class ShortEbpbFat32(ByteStruct):

    bpb_dos_331: BpbDos331
    fat_size_32: Annotated[int, 4]
    mirroring_flags: Annotated[int, 2]
    version: Annotated[int, 2]
    rootdir_start_cluster: Annotated[int, 4]
    fsinfo_sector: Annotated[int, 2]
    boot_sector_backup_start: Annotated[int, 2]
    reserved_1: Annotated[bytes, 12]
    physical_drive_number: Annotated[int, 1]
    reserved_2: Annotated[bytes, 1]
    extended_boot_signature: Annotated[bytes, 1]

    def validate(self) -> None:
        # previous BPBs
        if self.bpb_dos_331.bpb_dos_200.lss < MIN_LSS_FAT32:
            raise ValidationError(
                f'FAT32 requires a logical sector size of at least {MIN_LSS_FAT32} '
                f'bytes'
            )
        if self.bpb_dos_331.bpb_dos_200.rootdir_entries != 0:
            raise ValidationError('Root directory entry count must be 0')
        if self.bpb_dos_331.bpb_dos_200.total_size_200 != 0:
            raise ValidationError('Total size defined in DOS 2.0 BPB must be 0')
        if self.bpb_dos_331.bpb_dos_200.fat_size != 0:
            raise ValidationError('FAT size defined in DOS 2.0 BPB must be 0')

        # this EBPB
        if self.fat_size_32 <= 0:
            raise ValidationError('FAT size must be greater than 0')
        if self.version != FAT32_VERSION:
            raise ValidationError(f'Invalid FAT32 version {self.version}')
        if self.rootdir_start_cluster < 2:
            raise ValidationError(
                'Root directory start cluster must be greater than or equal to 2'
            )
        if self.fsinfo_available and self.fsinfo_sector != FS_INFO_SECTOR:
            raise ValidationError(
                f'FS information sector number must be {FS_INFO_SECTOR}'
            )

        # In this context, the term "boot sectors" refers to LBA 0, the FS information
        # sector and other reserved sectors present before any backup sector.
        min_boot_sectors = 1
        if self.fsinfo_available:
            min_boot_sectors = 2

        if self.backup_available:
            if self.boot_sector_backup_start < min_boot_sectors:
                raise ValidationError(
                    f'Boot sector backup start sector number must be greater than or '
                    f'equal to minimum boot sector count of {min_boot_sectors}'
                )
            boot_sectors = self.boot_sector_backup_start
            min_reserved = 2 * boot_sectors
        else:
            min_reserved = min_boot_sectors

        if self.bpb_dos_331.bpb_dos_200.reserved_size < min_reserved:
            raise ValidationError(
                f'Reserved sector count must be at least {min_reserved}'
            )
        _check_physical_drive_number(self.physical_drive_number)
        _check_extended_boot_signature(self.extended_boot_signature)

    @property
    def bpb_dos_200(self) -> BpbDos200:
        return self.bpb_dos_331.bpb_dos_200

    @property
    def total_size(self) -> int | None:
        return self.bpb_dos_331.total_size

    @property
    def fat_size(self) -> int:
        return self.fat_size_32

    @property
    def fsinfo_available(self) -> bool:
        return self.fsinfo_sector not in SECTOR_NUMBERS_UNUSED

    @property
    def backup_available(self) -> bool:
        return self.boot_sector_backup_start not in SECTOR_NUMBERS_UNUSED


@dataclass(frozen=True)
class EbpbFat(ByteStruct):
    """FAT12 and FAT16 extended BIOS parameter block."""

    short: ShortEbpbFat
    volume_id: Annotated[int, 4]
    volume_label: Annotated[bytes, 11]
    file_system_type: Annotated[bytes, 8]

    def validate(self) -> None:
        if self.short.extended_boot_signature != EXTENDED_BOOT_SIGNATURE_EXISTS:
            raise ValidationError(
                f'Extended boot signature must be {EXTENDED_BOOT_SIGNATURE_EXISTS!r} '
                f'to parse an extended FAT EBPB'
            )
        if self.file_system_type not in FILE_SYSTEM_TYPES_FAT:
            warnings.warn(
                f'Unknown file system type {self.file_system_type!r}; this might lead '
                f'some systems to refuse to recognize the file system',
                ValidationWarning,
            )

    @property
    def bpb_dos_200(self) -> BpbDos200:
        return self.short.bpb_dos_331.bpb_dos_200

    @property
    def total_size(self) -> int | None:
        return self.short.total_size

    @property
    def fat_size(self) -> int:
        return self.short.fat_size


@dataclass(frozen=True)
class EbpbFat32(ByteStruct):
    """FAT32 extended BIOS parameter block."""

    short: ShortEbpbFat32
    volume_id: Annotated[int, 4]
    volume_label: Annotated[bytes, 11]
    file_system_type: Annotated[bytes, 8]

    def validate(self) -> None:
        if self.short.extended_boot_signature != EXTENDED_BOOT_SIGNATURE_EXISTS:
            raise ValidationError(
                f'Extended boot signature must be {EXTENDED_BOOT_SIGNATURE_EXISTS!r} '
                f'to parse an extended FAT32 EBPB'
            )
        if self.file_system_type != FILE_SYSTEM_TYPE_FAT32:
            warnings.warn(
                f'Unknown file system type {self.file_system_type!r}; this might lead '
                f'some systems to refuse to recognize the file system',
                ValidationWarning,
            )

    def validate_for_volume(self, volume: Volume, *, recurse: bool = False) -> None:
        super().validate_for_volume(volume, recurse=recurse)
        if self.total_size is not None and self.total_size > volume.size_lba:
            raise ValidationError('Total size must not be greater than volume size')

    @property
    def bpb_dos_200(self) -> BpbDos200:
        return self.short.bpb_dos_331.bpb_dos_200

    @property
    def total_size(self) -> int | None:
        total_size_short = self.short.total_size
        if total_size_short is None:
            # If both total logical sectors entries at offset 0x20 and 0x13 are 0,
            # volumes can use file_system_type as a 64-bit total logical sectors entry.
            total_size = int.from_bytes(self.file_system_type, 'little')
            return total_size or None
        return total_size_short

    @property
    def fat_size(self) -> int:
        return self.short.fat_size

    @property
    def fsinfo_available(self) -> bool:
        return self.short.fsinfo_available

    @property
    def backup_available(self) -> bool:
        return self.short.backup_available


@dataclass(frozen=True)
class BootSectorStart(ByteStruct):

    jump_instruction: Annotated[bytes, 3]
    oem_name: Annotated[bytes, 8]

    def validate(self) -> None:
        if not any(
            self.jump_instruction.startswith(jump_start)
            for jump_start in JUMP_INSTRUCTIONS_START
        ):
            warnings.warn(
                'Unknown jump instruction pattern; this might lead some systems to '
                'refuse to recognize the file system',
                ValidationWarning,
            )
        if self.oem_name not in OEM_NAMES_COMMON:
            warnings.warn(
                'Unknown OEM name in boot sector; this might lead some systems to '
                'refuse to recognize the file system',
                ValidationWarning,
            )


# noinspection PyTypeChecker
BPB_PARSE_ORDER: tuple[Type[Bpb], ...] = (
    EbpbFat32,
    EbpbFat,
    ShortEbpbFat32,
    ShortEbpbFat,
    BpbDos331,
    BpbDos200,
)


@dataclass(frozen=True)
class BootSector:
    """FAT boot sector.

    Not a ``ByteStruct`` because of its dynamic nature.
    """

    start: BootSectorStart
    bpb: Bpb
    boot_code: bytes

    SIZE: ClassVar[int] = 512

    @classmethod
    def from_bytes(cls, b: bytes, custom_bpb_type: Type[Bpb] = None) -> BootSector:
        """Parse boot sector from ``bytes``.

        If ``custom_bpb_type`` is set, it is tried to parse the BIOS parameter block
        according to the given type. If ``custom_bpb_type`` is not set, it is tried
        to parse the BPB in the order defined in ``BPB_PARSE_ORDER``. If this fails,
        ``ValidationError`` is raised.
        """
        if len(b) != cls.SIZE:
            raise ValueError(
                f'Boot sector must be {cls.SIZE} bytes long, got {len(b)} bytes'
            )

        signature_size = len(SIGNATURE)
        signature = b[-signature_size:]
        if signature != SIGNATURE:
            raise ValidationError(f'Invalid VBR signature {signature!r}')

        start_size = len(BootSectorStart)
        start = BootSectorStart.from_bytes(b[:start_size])

        def parse_bpb(bpb_type: Type[Bpb]) -> Bpb:
            bpb_size = len(bpb_type)
            bpb_end = start_size + bpb_size
            return bpb_type.from_bytes(b[start_size:bpb_end])

        bpb: Bpb | None = None
        if custom_bpb_type is not None:
            # noinspection PyTypeChecker
            bpb = parse_bpb(custom_bpb_type)
        else:
            for bpb_type_ in BPB_PARSE_ORDER:
                try:
                    # noinspection PyTypeChecker
                    bpb = parse_bpb(bpb_type_)
                    break
                except ValidationError:
                    pass

        if bpb is None:
            raise ValidationError('No known BPB could be parsed')

        boot_code_start = start_size + len(bpb)
        boot_code = b[boot_code_start:-signature_size]
        return cls(start, bpb, boot_code)

    def __bytes__(self) -> bytes:
        return bytes(self.start) + bytes(self.bpb) + self.boot_code + SIGNATURE

    def __len__(self) -> int:
        return self.SIZE

    def validate(self) -> None:
        len_all = len(self.start) + len(self.bpb) + len(self.boot_code) + len(SIGNATURE)
        if len_all != self.SIZE:
            raise ValidationError(
                f'Invalid size of boot sector (expected {self.SIZE} bytes, got '
                f'{len_all} bytes'
            )
        if self.total_clusters < 1:  # also triggers validation of total_size
            raise ValidationError('Total cluster count must be greater than 0')

        fat_32_bpb = isinstance(self.bpb, (EbpbFat32, ShortEbpbFat32))
        fat_32_detected = self.fat_type is FatType.FAT_32
        if fat_32_bpb != fat_32_detected:
            raise ValidationError('Detected FAT type does not match BPB')

        if not self.boot_code.strip(b'\x00'):
            raise ValidationError(
                f'Boot code must not be empty, use at least a dummy boot loader such '
                f'as {BOOT_CODE_DUMMY!r}'
            )

    def validate_for_volume(self, volume: Volume, *, recurse: bool = False) -> None:
        if recurse:
            self.start.validate_for_volume(volume, recurse=True)
            self.bpb.validate_for_volume(volume, recurse=True)

    def __post_init__(self) -> None:
        self.validate()

    @property
    def total_size(self) -> int:
        """Total size occupied by the file system in sectors."""
        if self.bpb.total_size is None:
            raise ValidationError('No total size was defined')
        return self.bpb.total_size

    @property
    def fat_size(self) -> int:
        """Size of a file allocation table in sectors."""
        return self.bpb.fat_size

    @property
    def fat_region_start(self) -> int:
        """First sector of FAT region."""
        return self.bpb.bpb_dos_200.reserved_size

    @property
    def fat_region_size(self) -> int:
        """Size of FAT region in sectors."""
        return self.bpb.bpb_dos_200.fat_count * self.bpb.fat_size

    @property
    def rootdir_region_start(self) -> int:
        """First sector of root directory region."""
        return self.fat_region_start + self.fat_region_size

    @property
    def rootdir_region_size(self) -> int:
        """Size of root directory region in sectors. Always zero for FAT32."""
        entries = self.bpb.bpb_dos_200.rootdir_entries
        lss = self.bpb.bpb_dos_200.lss
        # entries already align to lss of disk (checked in BpbDos200)
        return (entries * DIRECTORY_ENTRY_SIZE) // lss

    @property
    def data_region_start(self) -> int:
        """First sector of data region."""
        return self.rootdir_region_start + self.rootdir_region_size

    @property
    def data_region_size(self) -> int:
        """Size of data region in sectors."""
        return self.total_size - self.data_region_start

    @property
    def cluster_size(self) -> int:
        return self.bpb.bpb_dos_200.cluster_size

    @property
    def total_clusters(self) -> int:
        return self.data_region_size // self.cluster_size

    @property
    def fat_type(self) -> FatType:
        if self.total_clusters < 4085:
            return FatType.FAT_12
        elif self.total_clusters < 65525:
            return FatType.FAT_16
        else:
            return FatType.FAT_32


@dataclass(frozen=True)
class FsInfo(ByteStruct):
    """FS information sector (FAT32 only)."""

    signature_1: Annotated[bytes, 4]
    reserved_1: Annotated[bytes, 480]
    signature_2: Annotated[bytes, 4]
    free_clusters: Annotated[int, 4]
    last_allocated_cluster: Annotated[int, 4]
    reserved_2: Annotated[bytes, 12]
    signature_3: Annotated[bytes, 4]

    def validate(self) -> None:
        if self.signature_1 != FS_INFO_SIGNATURE_1:
            raise ValidationError(
                f'Invalid first FS information sector signature {self.signature_1!r}'
            )
        if self.signature_2 != FS_INFO_SIGNATURE_2:
            raise ValidationError(
                f'Invalid second FS information sector signature {self.signature_2!r}'
            )
        if self.signature_3 != FS_INFO_SIGNATURE_3:
            raise ValidationError(
                f'Invalid third FS information sector signature {self.signature_3!r}'
            )
