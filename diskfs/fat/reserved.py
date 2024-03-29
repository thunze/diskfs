"""Structures found in the reserved region of a FAT file system."""

from __future__ import annotations

import warnings
from dataclasses import dataclass

# noinspection PyUnresolvedReferences, PyProtectedMember
from typing import TYPE_CHECKING, ClassVar, Protocol, _ProtocolMeta

from typing_extensions import Annotated

from ..base import ValidationError, ValidationWarning, is_power_of_two
from ..bytestruct import ByteStruct
from .base import FatType

if TYPE_CHECKING:
    from ..volume import Volume

__all__ = [
    "Bpb",
    "BpbDos200",
    "BpbDos331",
    "ShortEbpbFat",
    "ShortEbpbFat32",
    "EbpbFat",
    "EbpbFat32",
    "BootSectorStart",
    "BootSector",
    "FsInfoSector",
    "EXTENDED_BOOT_SIGNATURE_EXISTS",
    "FILE_SYSTEM_TYPE_FAT32",
    "FAT32_VERSION",
    "FS_INFO_SECTOR",
    "SIGNATURE",
    "FS_INFO_SIGNATURE_1",
    "FS_INFO_SIGNATURE_2",
    "FS_INFO_SIGNATURE_3",
    "FS_INFO_UNKNOWN",
    "CLUSTER_SIZE_DEFAULT",
    "ROOTDIR_ENTRIES_DEFAULT",
    "MEDIA_TYPE_DEFAULT",
    "SECTORS_PER_TRACK_DEFAULT",
    "HEADS_DEFAULT",
    "PHYSICAL_DRIVE_NUMBER_DEFAULT",
    "VOLUME_LABEL_DEFAULT",
    "BOOT_CODE_DUMMY",
]


MIN_LSS_FAT = 128
MIN_LSS_FAT32 = 512
SECTORS_PER_TRACK_MAX = 63
HEADS_MAX = 255
PHYSICAL_DRIVE_NUMBERS_RESERVED = (0x7F, 0xFF)
EXTENDED_BOOT_SIGNATURES = (b"\x28", b"\x29")
EXTENDED_BOOT_SIGNATURE_EXISTS = b"\x29"  # EBPB extension exists
FILE_SYSTEM_TYPES_FAT = (b"FAT12   ", b"FAT16   ", b"FAT     ")
FILE_SYSTEM_TYPE_FAT32 = b"FAT32   "
FAT32_VERSION = 0
SECTOR_NUMBERS_UNUSED = (0, 0xFFFF)
FS_INFO_SECTOR = 1

JUMP_INSTRUCTIONS_START = (b"\xEB", b"\xE9", b"\x90\xEB")
OEM_NAMES_COMMON = (
    b"MSDOS5.0",
    b"MSWIN4.1",
    b"IBM  3.3",
    b"IBM  7.1",
    b"mkdosfs ",
    b"FreeDOS ",
)
SIGNATURE = b"\x55\xaa"

# FS information sector
FS_INFO_SIGNATURE_1 = b"RRaA"
FS_INFO_SIGNATURE_2 = b"rrAa"
FS_INFO_SIGNATURE_3 = b"\x00\x00" + SIGNATURE
FS_INFO_UNKNOWN = 0xFFFFFFFF

# Defaults
CLUSTER_SIZE_DEFAULT = 16
ROOTDIR_ENTRIES_DEFAULT = 240
MEDIA_TYPE_DEFAULT = 0xF8
SECTORS_PER_TRACK_DEFAULT = 63
HEADS_DEFAULT = 255
PHYSICAL_DRIVE_NUMBER_DEFAULT = 0x80
VOLUME_LABEL_DEFAULT = b"NO NAME    "
BOOT_CODE_DUMMY = b"\xF4\xEB\xFD"  # endless loop

# Other constants
DIRECTORY_ENTRY_SIZE = 32


class _BpbMeta(_ProtocolMeta):  # pragma: no cover
    """Meta-protocol to enable structural subtyping of BIOS parameter block classes."""

    # noinspection PyMethodParameters
    def __len__(cls) -> int:  # type: ignore[empty-body]
        """Size of the BPB in bytes."""
        ...


# noinspection PyPropertyDefinition
class Bpb(Protocol, metaclass=_BpbMeta):  # pragma: no cover
    """BIOS parameter block."""

    @classmethod
    def from_bytes(cls, b: bytes) -> Bpb:
        """Parse BPB from `bytes`."""
        ...

    def __bytes__(self) -> bytes:
        """`bytes` form of the BPB."""
        ...

    def __len__(self) -> int:
        """Size of the BPB in bytes."""
        ...

    def validate_for_volume(self, volume: Volume) -> None:
        """Validate the BPB against the metadata of `volume`."""
        ...

    @property
    def bpb_dos_200(self) -> BpbDos200:
        """Encapsulated DOS 2.0 BPB."""
        ...

    @property
    def total_size(self) -> int | None:
        """Total size of the file system described by the BPB in logical sectors.

        `None` if the total size is not defined in this BPB.
        """
        ...

    @property
    def fat_size(self) -> int:
        """Size of a FAT of the file system in logical sectors."""
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
                f"Logical sector size must be greater than or equal to "
                f"{DIRECTORY_ENTRY_SIZE}"
            )
        if not is_power_of_two(self.lss):
            raise ValidationError("Logical sector size must be a power of 2")
        if self.cluster_size <= 0:
            raise ValidationError("Cluster size must be greater than 0")
        if not is_power_of_two(self.cluster_size):
            raise ValidationError("Cluster size must be a power of 2")
        if self.reserved_size < 1:
            raise ValidationError("Reserved sector count must be greater than 0")
        if self.fat_count < 1:
            raise ValidationError("FAT count must be greater than 0")
        if (self.rootdir_entries * DIRECTORY_ENTRY_SIZE) % self.lss != 0:
            raise ValidationError(
                "Root directory entries must align with logical sector size"
            )
        if self.media_type <= 0xEF or (0xF1 <= self.media_type <= 0xF7):
            raise ValidationError(f"Unsupported media type 0x{self.media_type:x}")

    def validate_for_volume(self, volume: Volume) -> None:
        if self.lss != volume.sector_size.logical:
            raise ValidationError(
                "Logical sector size defined in DOS 2.0 BPB does not match logical "
                "sector size of disk"
            )
        if self.total_size_200 > volume.size_lba:
            raise ValidationError("Total size must not be greater than volume size")

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
                f"Sector count per track must be less than or equal to "
                f"{SECTORS_PER_TRACK_MAX}"
            )
        if self.heads > HEADS_MAX:
            raise ValidationError(
                f"Head count must be a less than or equal to {HEADS_MAX}"
            )

        # Total sizes must match if none of them is 0
        total_size_200 = self.bpb_dos_200_.total_size
        total_size_331 = self.total_size_331
        if total_size_200 and total_size_331 and total_size_200 != total_size_331:
            raise ValidationError(
                "Total size does not match total size defined in DOS 2.0 BPB"
            )

    def validate_for_volume(self, volume: Volume) -> None:
        self.bpb_dos_200_.validate_for_volume(volume)
        if self.hidden_before_partition != volume.start_lba:
            raise ValidationError(
                "Hidden sector count does not match volume start sector"
            )
        if self.total_size_331 > volume.size_lba:
            raise ValidationError("Total size must not be greater than volume size")

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
    """Issue `ValidationWarning` if `physical_drive_number` is a reserved EBPB
    physical drive number value.
    """
    if physical_drive_number in PHYSICAL_DRIVE_NUMBERS_RESERVED:
        warnings.warn(
            f"Reserved physical drive number {physical_drive_number}", ValidationWarning
        )


def _check_extended_boot_signature(extended_boot_signature: bytes) -> None:
    """Raise `ValidationError` if `extended_boot_signature` is an invalid EBPB
    extended boot signature.
    """
    if extended_boot_signature not in EXTENDED_BOOT_SIGNATURES:
        raise ValidationError(
            f"Invalid extended boot signature {extended_boot_signature!r}"
        )


@dataclass(frozen=True)
class ShortEbpbFat(ByteStruct):
    """Shortened FAT12/16 extended BIOS parameter block."""

    bpb_dos_331: BpbDos331
    physical_drive_number: Annotated[int, 1]
    reserved: Annotated[int, 1]
    extended_boot_signature: Annotated[bytes, 1]

    def validate(self) -> None:
        # Previous BPBs
        if self.bpb_dos_331.bpb_dos_200.lss < MIN_LSS_FAT:
            raise ValidationError(
                f"FAT requires a logical sector size of at least {MIN_LSS_FAT} bytes"
            )
        if self.bpb_dos_331.bpb_dos_200.rootdir_entries <= 0:
            raise ValidationError("Root directory entry count must be greater than 0")
        if self.bpb_dos_331.bpb_dos_200.fat_size <= 0:
            raise ValidationError(
                "FAT size defined in DOS 2.0 BPB must be greater than 0"
            )

        # This EBPB
        _check_physical_drive_number(self.physical_drive_number)
        _check_extended_boot_signature(self.extended_boot_signature)

    def validate_for_volume(self, volume: Volume) -> None:
        self.bpb_dos_331.validate_for_volume(volume)

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
    """Shortened FAT32 extended BIOS parameter block."""

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
        # Previous BPBs
        if self.bpb_dos_331.bpb_dos_200.lss < MIN_LSS_FAT32:
            raise ValidationError(
                f"FAT32 requires a logical sector size of at least {MIN_LSS_FAT32} "
                f"bytes"
            )
        if self.bpb_dos_331.bpb_dos_200.rootdir_entries != 0:
            raise ValidationError("Root directory entry count must be 0")
        if self.bpb_dos_331.bpb_dos_200.total_size_200 != 0:
            raise ValidationError("Total size defined in DOS 2.0 BPB must be 0")
        if self.bpb_dos_331.bpb_dos_200.fat_size != 0:
            raise ValidationError("FAT size defined in DOS 2.0 BPB must be 0")

        # This EBPB
        if self.fat_size_32 <= 0:
            raise ValidationError("FAT size must be greater than 0")
        if self.version != FAT32_VERSION:
            raise ValidationError(f"Invalid FAT32 version {self.version}")
        if self.rootdir_start_cluster < 2:
            raise ValidationError(
                "Root directory start cluster must be greater than or equal to 2"
            )
        if self.fsinfo_available and self.fsinfo_sector != FS_INFO_SECTOR:
            raise ValidationError(
                f"FS information sector number must be {FS_INFO_SECTOR}"
            )

        # In this context, the term "boot sectors" refers to LBA 0, the FS information
        # sector and other reserved sectors present before any backup sector.
        min_boot_sectors = 1
        if self.fsinfo_available:
            min_boot_sectors = 2

        if self.backup_available:
            if self.boot_sector_backup_start < min_boot_sectors:
                raise ValidationError(
                    f"Boot sector backup start sector number must be greater than or "
                    f"equal to minimum boot sector count of {min_boot_sectors}"
                )
            boot_sectors = self.boot_sector_backup_start
            min_reserved = 2 * boot_sectors
        else:
            min_reserved = min_boot_sectors

        if self.bpb_dos_331.bpb_dos_200.reserved_size < min_reserved:
            raise ValidationError(
                f"Reserved sector count must be at least {min_reserved}"
            )
        _check_physical_drive_number(self.physical_drive_number)
        _check_extended_boot_signature(self.extended_boot_signature)

    def validate_for_volume(self, volume: Volume) -> None:
        self.bpb_dos_331.validate_for_volume(volume)

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
        """Whether an FS information sector is present in the reserved region."""
        return self.fsinfo_sector not in SECTOR_NUMBERS_UNUSED

    @property
    def backup_available(self) -> bool:
        """Whether a backup of the boot sectors is present in the reserved region.

        In this context, the term "boot sectors" refers to LBA 0, the FS information
        sector and other reserved sectors present in the reserved region before any
        backup sector.
        """
        return self.boot_sector_backup_start not in SECTOR_NUMBERS_UNUSED


@dataclass(frozen=True)
class EbpbFat(ByteStruct):
    """FAT12/16 extended BIOS parameter block."""

    short: ShortEbpbFat
    volume_id: Annotated[int, 4]
    volume_label: Annotated[bytes, 11]
    file_system_type: Annotated[bytes, 8]

    def validate(self) -> None:
        if self.short.extended_boot_signature != EXTENDED_BOOT_SIGNATURE_EXISTS:
            raise ValidationError(
                f"Extended boot signature must be {EXTENDED_BOOT_SIGNATURE_EXISTS!r} "
                f"to parse an extended FAT EBPB"
            )
        if self.file_system_type not in FILE_SYSTEM_TYPES_FAT:
            warnings.warn(
                f"Unknown file system type {self.file_system_type!r}; this might lead "
                f"some systems to refuse to recognize the file system",
                ValidationWarning,
            )

    def validate_for_volume(self, volume: Volume) -> None:
        self.short.validate_for_volume(volume)

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
                f"Extended boot signature must be {EXTENDED_BOOT_SIGNATURE_EXISTS!r} "
                f"to parse an extended FAT32 EBPB"
            )
        if self.file_system_type != FILE_SYSTEM_TYPE_FAT32:
            warnings.warn(
                f"Unknown file system type {self.file_system_type!r}; this might lead "
                f"some systems to refuse to recognize the file system",
                ValidationWarning,
            )

    def validate_for_volume(self, volume: Volume) -> None:
        self.short.validate_for_volume(volume)
        if self.total_size is not None and self.total_size > volume.size_lba:
            raise ValidationError("Total size must not be greater than volume size")

    @property
    def bpb_dos_200(self) -> BpbDos200:
        return self.short.bpb_dos_331.bpb_dos_200

    @property
    def total_size(self) -> int | None:
        total_size_short = self.short.total_size
        if total_size_short is None:
            # If both total logical sectors entries at offset 0x20 and 0x13 are 0,
            # volumes can use file_system_type as a 64-bit total logical sectors entry.
            total_size = int.from_bytes(self.file_system_type, "little")
            return total_size or None
        return total_size_short

    @property
    def fat_size(self) -> int:
        return self.short.fat_size

    @property
    def fsinfo_available(self) -> bool:
        """Whether an FS information sector is present in the reserved region."""
        return self.short.fsinfo_available

    @property
    def backup_available(self) -> bool:
        """Whether a backup of the boot sectors is present in the reserved region.

        In this context, the term "boot sectors" refers to LBA 0, the FS information
        sector and other reserved sectors present in the reserved region before any
        backup sector.
        """
        return self.short.backup_available


@dataclass(frozen=True)
class BootSectorStart(ByteStruct):
    """First eleven bytes of a FAT boot sector."""

    jump_instruction: Annotated[bytes, 3]
    oem_name: Annotated[bytes, 8]

    def validate(self) -> None:
        if not any(
            self.jump_instruction.startswith(jump_start)
            for jump_start in JUMP_INSTRUCTIONS_START
        ):
            warnings.warn(
                "Unknown jump instruction pattern; this might lead some systems to "
                "refuse to recognize the file system",
                ValidationWarning,
            )
        if self.oem_name not in OEM_NAMES_COMMON:
            warnings.warn(
                "Unknown OEM name in boot sector; this might lead some systems to "
                "refuse to recognize the file system",
                ValidationWarning,
            )


# noinspection PyTypeChecker
BPB_PARSE_ORDER: tuple[type[Bpb], ...] = (
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

    Not a `ByteStruct` because of its dynamic nature.
    """

    start: BootSectorStart
    bpb: Bpb
    boot_code: bytes

    SIZE: ClassVar[int] = 512

    @classmethod
    def from_bytes(
        cls, b: bytes, custom_bpb_type: type[Bpb] | None = None
    ) -> BootSector:
        """Parse boot sector from `bytes`.

        If `custom_bpb_type` is set, it is tried to parse the BIOS parameter block
        according to the given type. If `custom_bpb_type` is not set, it is tried
        to parse the BPB in the order defined in `BPB_PARSE_ORDER`. If this fails,
        `ValidationError` is raised.
        """
        if len(b) != cls.SIZE:
            raise ValueError(
                f"Boot sector must be {cls.SIZE} bytes long, got {len(b)} bytes"
            )

        signature_size = len(SIGNATURE)
        signature = b[-signature_size:]
        if signature != SIGNATURE:
            raise ValidationError(f"Invalid VBR signature {signature!r}")

        start_size = len(BootSectorStart)
        start = BootSectorStart.from_bytes(b[:start_size])

        def parse_bpb(bpb_type: type[Bpb]) -> Bpb:
            """Parse BPB of type `bpb_type` from `b`."""
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
            raise ValidationError("No known FAT BPB could be parsed")

        boot_code_start = start_size + len(bpb)
        boot_code = b[boot_code_start:-signature_size]
        return cls(start, bpb, boot_code)

    def __bytes__(self) -> bytes:
        """`bytes` form of the boot sector."""
        return bytes(self.start) + bytes(self.bpb) + self.boot_code + SIGNATURE

    def __len__(self) -> int:
        """Size of the boot sector in bytes."""
        return self.SIZE

    def validate(self) -> None:
        """Validation logic."""
        len_all = len(self.start) + len(self.bpb) + len(self.boot_code) + len(SIGNATURE)
        if len_all != self.SIZE:
            raise ValidationError(
                f"Invalid size of boot sector (expected {self.SIZE} bytes, got "
                f"{len_all} bytes"
            )
        if self.total_clusters < 1:  # Also triggers validation of total_size
            raise ValidationError("Total cluster count must be greater than 0")

        fat_32_bpb = isinstance(self.bpb, (EbpbFat32, ShortEbpbFat32))
        fat_32_detected = self.fat_type is FatType.FAT_32
        if fat_32_bpb != fat_32_detected:
            raise ValidationError("Detected FAT type does not match BPB")

        if not self.boot_code.strip(b"\x00"):
            warnings.warn(
                f"Boot code should not be empty, use at least a dummy boot loader, "
                f"such as {BOOT_CODE_DUMMY!r}",
                ValidationWarning,
            )

    def validate_for_volume(self, volume: Volume) -> None:
        """Validate the boot sector against the metadata of `volume`."""
        self.bpb.validate_for_volume(volume)

    def __post_init__(self) -> None:
        """Execute validation logic after instance creation."""
        self.validate()

    @property
    def total_size(self) -> int:
        """Total size of the file system in sectors."""
        if self.bpb.total_size is None:
            raise ValidationError("No total size was defined")
        return self.bpb.total_size

    @property
    def fat_size(self) -> int:
        """Size of a FAT of the file system in sectors."""
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
        # Alignment to LSS of disk was already checked in BpbDos200
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
        """Size of a cluster in sectors."""
        return self.bpb.bpb_dos_200.cluster_size

    @property
    def total_clusters(self) -> int:
        """Total clusters provided by the file system."""
        return self.data_region_size // self.cluster_size

    @property
    def fat_type(self) -> FatType:
        """Type of FAT file system (FAT12, FAT16 or FAT32) according to the amount of
        clusters provided by the file system.
        """
        if self.total_clusters < 4085:
            return FatType.FAT_12
        elif self.total_clusters < 65525:
            return FatType.FAT_16
        else:
            return FatType.FAT_32


@dataclass(frozen=True)
class FsInfoSector(ByteStruct):
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
                f"Invalid first FS information sector signature {self.signature_1!r}"
            )
        if self.signature_2 != FS_INFO_SIGNATURE_2:
            raise ValidationError(
                f"Invalid second FS information sector signature {self.signature_2!r}"
            )
        if self.signature_3 != FS_INFO_SIGNATURE_3:
            raise ValidationError(
                f"Invalid third FS information sector signature {self.signature_3!r}"
            )
