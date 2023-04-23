"""Tests for the ``reserved`` module of the ``fat`` package."""

import warnings
from dataclasses import replace

import pytest

from diskfs.base import SectorSize, ValidationError, ValidationWarning
from diskfs.fat.reserved import (
    CLUSTER_SIZE_DEFAULT,
    EXTENDED_BOOT_SIGNATURE_EXISTS,
    FAT32_VERSION,
    FILE_SYSTEM_TYPE_FAT32,
    FS_INFO_SECTOR,
    HEADS_DEFAULT,
    MEDIA_TYPE_DEFAULT,
    PHYSICAL_DRIVE_NUMBER_DEFAULT,
    ROOTDIR_ENTRIES_DEFAULT,
    SECTORS_PER_TRACK_DEFAULT,
    VOLUME_LABEL_DEFAULT,
    BpbDos200,
    BpbDos331,
    EbpbFat,
    EbpbFat32,
    ShortEbpbFat,
    ShortEbpbFat32,
)
from diskfs.volume import Volume


@pytest.fixture
def volume_meta(request, monkeypatch):
    """Fixture providing a surface-level mocked instance of ``Volume`` with
    customizable ``start_lba``, ``end_lba`` and ``sector_size`` values.

    Parametrized using a ``tuple`` of the desired values for ``start_lba``,
    ``end_lba``, ``sector_size.logical`` and ``sector_size.physical``.
    """
    start, end, lss, pss = request.param
    size = end - start + 1
    monkeypatch.setattr('diskfs.volume.Volume.__init__', lambda self: None)
    monkeypatch.setattr('diskfs.volume.Volume.start_lba', property(lambda self: start))
    monkeypatch.setattr('diskfs.volume.Volume.end_lba', property(lambda self: end))
    monkeypatch.setattr('diskfs.volume.Volume.size_lba', property(lambda self: size))
    monkeypatch.setattr(
        'diskfs.volume.Volume.sector_size', property(lambda self: SectorSize(lss, pss))
    )
    return Volume()  # type: ignore[call-arg]


# Example BPBs with sane default values

BPB_DOS_200_FAT12_EXAMPLE = BpbDos200(
    lss=512,
    cluster_size=CLUSTER_SIZE_DEFAULT,
    reserved_size=1,
    fat_count=1,
    rootdir_entries=ROOTDIR_ENTRIES_DEFAULT,  # 15 sectors
    total_size_200=40960,  # 1 + 8 + 15 + 40936 sectors
    media_type=MEDIA_TYPE_DEFAULT,
    fat_size_200=8,
)
BPB_DOS_200_FAT16_EXAMPLE = BpbDos200(
    lss=512,
    cluster_size=CLUSTER_SIZE_DEFAULT,
    reserved_size=2,
    fat_count=2,
    rootdir_entries=ROOTDIR_ENTRIES_DEFAULT,  # 15 sectors
    total_size_200=0,
    media_type=MEDIA_TYPE_DEFAULT,
    fat_size_200=32,
)
BPB_DOS_200_FAT32_EXAMPLE = BpbDos200(
    lss=512,
    cluster_size=CLUSTER_SIZE_DEFAULT,
    reserved_size=6,
    fat_count=2,
    rootdir_entries=0,  # 0 sectors
    total_size_200=0,
    media_type=MEDIA_TYPE_DEFAULT,
    fat_size_200=0,
)

BPB_DOS_331_FAT12_EXAMPLE = BpbDos331(
    bpb_dos_200_=BPB_DOS_200_FAT12_EXAMPLE,
    sectors_per_track=SECTORS_PER_TRACK_DEFAULT,
    heads=HEADS_DEFAULT,
    hidden_before_partition=0,
    total_size_331=0,
)
BPB_DOS_331_FAT16_EXAMPLE = BpbDos331(
    bpb_dos_200_=BPB_DOS_200_FAT16_EXAMPLE,
    sectors_per_track=SECTORS_PER_TRACK_DEFAULT,
    heads=HEADS_DEFAULT,
    hidden_before_partition=0,
    total_size_331=131072,  # 2 + 2 * 32 + 15 + 130991 sectors
)
BPB_DOS_331_FAT32_EXAMPLE = BpbDos331(
    bpb_dos_200_=BPB_DOS_200_FAT32_EXAMPLE,
    sectors_per_track=SECTORS_PER_TRACK_DEFAULT,
    heads=HEADS_DEFAULT,
    hidden_before_partition=0,
    total_size_331=2097152,  # 6 + 2 * 1024 + 0 + 2095098 sectors
)

SHORT_EBPB_FAT12_EXAMPLE = ShortEbpbFat(
    bpb_dos_331=BPB_DOS_331_FAT12_EXAMPLE,
    physical_drive_number=PHYSICAL_DRIVE_NUMBER_DEFAULT,
    reserved=0,
    extended_boot_signature=EXTENDED_BOOT_SIGNATURE_EXISTS,
)
SHORT_EBPB_FAT16_EXAMPLE = ShortEbpbFat(
    bpb_dos_331=BPB_DOS_331_FAT16_EXAMPLE,
    physical_drive_number=PHYSICAL_DRIVE_NUMBER_DEFAULT,
    reserved=0,
    extended_boot_signature=EXTENDED_BOOT_SIGNATURE_EXISTS,
)
SHORT_EBPB_FAT32_EXAMPLE = ShortEbpbFat32(
    bpb_dos_331=BPB_DOS_331_FAT32_EXAMPLE,
    fat_size_32=1024,
    mirroring_flags=0,
    version=FAT32_VERSION,
    rootdir_start_cluster=2,
    fsinfo_sector=FS_INFO_SECTOR,
    boot_sector_backup_start=3,
    reserved_1=b'\x00' * 12,
    physical_drive_number=PHYSICAL_DRIVE_NUMBER_DEFAULT,
    reserved_2=b'\x00',
    extended_boot_signature=EXTENDED_BOOT_SIGNATURE_EXISTS,
)

EBPB_FAT12_EXAMPLE = EbpbFat(
    short=SHORT_EBPB_FAT12_EXAMPLE,
    volume_id=2974034791,
    volume_label=VOLUME_LABEL_DEFAULT,
    file_system_type=b'FAT12   ',
)
EBPB_FAT16_EXAMPLE = EbpbFat(
    short=SHORT_EBPB_FAT16_EXAMPLE,
    volume_id=727423026,
    volume_label=VOLUME_LABEL_DEFAULT,
    file_system_type=b'FAT16   ',
)
EBPB_FAT32_EXAMPLE = EbpbFat32(
    short=SHORT_EBPB_FAT32_EXAMPLE,
    volume_id=492245945,
    volume_label=VOLUME_LABEL_DEFAULT,
    file_system_type=FILE_SYSTEM_TYPE_FAT32,
)


class TestBpbDos200:
    """Tests for ``BpbDos200``."""

    @pytest.mark.parametrize(
        'replace_kwargs',
        [{'lss': 1 << (exp + 5), 'rootdir_entries': 1 << exp} for exp in range(0, 9)]
        + [{'cluster_size': 1 << exp} for exp in range(0, 8)]
        + [{'reserved_size': size} for size in range(1, 7)]
        + [
            {'fat_count': 1},
            {'fat_count': 2},
            {'media_type': 0xF0},
            {'media_type': 0xF8},
            {'media_type': 0xF9},
            {'media_type': 0xFF},
        ],
    )
    def test_validate_success(self, replace_kwargs):
        """Test custom validation logic for succeeding cases."""
        replace(BPB_DOS_200_FAT16_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['replace_kwargs', 'msg_contains'],
        [
            ({'lss': 16}, 'Logical sector size'),
            ({'lss': 48}, 'Logical sector size'),
            ({'cluster_size': 0}, 'Cluster size'),
            ({'cluster_size': 3}, 'Cluster size'),
            ({'reserved_size': 0}, 'Reserved sector count'),
            ({'fat_count': 0}, 'FAT count'),
            ({'media_type': 0}, 'media type'),
            ({'media_type': 0xA0}, 'media type'),
            ({'media_type': 0xEF}, 'media type'),
            ({'media_type': 0xF1}, 'media type'),
            ({'media_type': 0xF3}, 'media type'),
            ({'media_type': 0xF7}, 'media type'),
        ],
    )
    def test_validate_fail(self, replace_kwargs, msg_contains):
        """Test custom validation logic for failing cases."""
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            replace(BPB_DOS_200_FAT16_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['lss', 'rootdir_entries'],
        [
            (32, 0),
            (32, 1),
            (32, 2),
            (32, 3),
            (64, 2),
            (64, 4),
            (64, 6),
            (256, 8),
            (256, 16),
            (256, 24),
            (512, 16),
            (512, 32),
            (512, 48),
            (4096, 128),
            (4096, 256),
            (4096, 384),
        ],
    )
    def test_validate_rootdir_entries_success(self, lss, rootdir_entries):
        """Test that validation succeeds for valid combinations of values for LSS and
        root directory entry count.
        """
        replace(BPB_DOS_200_FAT16_EXAMPLE, lss=lss, rootdir_entries=rootdir_entries)

    @pytest.mark.parametrize(
        ['lss', 'rootdir_entries'],
        [
            (64, 1),
            (64, 3),
            (64, 7),
            (256, 2),
            (256, 4),
            (256, 12),
            (512, 2),
            (512, 8),
            (512, 24),
            (4096, 64),
            (4096, 192),
        ],
    )
    def test_validate_rootdir_entries_fail(self, lss, rootdir_entries):
        """Test that validation fails for invalid combinations of values for LSS and
        root directory entry count.
        """
        with pytest.raises(ValidationError, match='.*Root directory entries.*'):
            replace(BPB_DOS_200_FAT16_EXAMPLE, lss=lss, rootdir_entries=rootdir_entries)

    @pytest.mark.parametrize(
        ['volume_meta', 'replace_kwargs'],
        [
            ((0, 2047, 512, 512), {'lss': 512, 'rootdir_entries': 16}),
            ((0, 2047, 512, 4096), {'lss': 512, 'rootdir_entries': 16}),
            ((0, 2047, 4096, 4096), {'lss': 4096, 'rootdir_entries': 128}),
            ((0, 2047, 512, 512), {'total_size_200': 1024}),
            ((0, 2047, 512, 512), {'total_size_200': 2048}),
            ((0, 4095, 512, 512), {'total_size_200': 1001}),
        ],
        indirect=['volume_meta'],
    )
    def test_validate_for_volume_success(self, volume_meta, replace_kwargs):
        """Test validation against a specific volume for succeeding cases."""
        bpb = replace(BPB_DOS_200_FAT16_EXAMPLE, **replace_kwargs)
        bpb.validate_for_volume(volume_meta)

    @pytest.mark.parametrize(
        ['volume_meta', 'replace_kwargs', 'msg_contains'],
        [
            (
                (0, 2048, 4096, 4096),
                {'lss': 512, 'rootdir_entries': 16},
                'Logical sector size.*disk',
            ),
            (
                (0, 2048, 512, 4096),
                {'lss': 4096, 'rootdir_entries': 128},
                'Logical sector size.*disk',
            ),
            ((0, 2047, 512, 512), {'total_size_200': 4096}, 'Total size.*volume'),
            ((0, 4095, 512, 512), {'total_size_200': 4097}, 'Total size.*volume'),
        ],
        indirect=['volume_meta'],
    )
    def test_validate_for_volume_fail(self, volume_meta, replace_kwargs, msg_contains):
        """Test validation against a specific volume for failing cases."""
        bpb = replace(BPB_DOS_200_FAT16_EXAMPLE, **replace_kwargs)
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            bpb.validate_for_volume(volume_meta)

    @pytest.mark.parametrize(
        ['bpb', 'total_size'],
        [
            (BPB_DOS_200_FAT12_EXAMPLE, 40960),
            (BPB_DOS_200_FAT16_EXAMPLE, None),
            (BPB_DOS_200_FAT32_EXAMPLE, None),
        ],
    )
    def test_properties(self, bpb, total_size):
        """Test that property values defined on BPBs match the expected values."""
        assert bpb.bpb_dos_200 is bpb
        assert bpb.total_size == total_size
        assert bpb.fat_size == bpb.fat_size_200


class TestBpbDos331:
    """Tests for ``BpbDos331``."""

    @pytest.mark.parametrize(
        'replace_kwargs',
        [
            {'sectors_per_track': 63},
            {'sectors_per_track': 26},
            {'heads': 255},
            {'heads': 2},
            {
                'bpb_dos_200_': replace(BPB_DOS_200_FAT12_EXAMPLE, total_size_200=8192),
                'total_size_331': 8192,
            },
            {
                'bpb_dos_200_': replace(BPB_DOS_200_FAT12_EXAMPLE, total_size_200=0),
                'total_size_331': 8192,
            },
            {
                'bpb_dos_200_': replace(BPB_DOS_200_FAT12_EXAMPLE, total_size_200=8192),
                'total_size_331': 0,
            },
            {
                'bpb_dos_200_': replace(BPB_DOS_200_FAT32_EXAMPLE, total_size_200=0),
                'total_size_331': 0,
            },
        ],
    )
    def test_validate_success(self, replace_kwargs):
        """Test custom validation logic for succeeding cases."""
        replace(BPB_DOS_331_FAT16_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['replace_kwargs', 'msg_contains'],
        [
            ({'sectors_per_track': 64}, 'Sector count per track'),
            ({'sectors_per_track': 65}, 'Sector count per track'),
            ({'heads': 256}, 'Head count'),
            ({'heads': 257}, 'Head count'),
            (
                {
                    'bpb_dos_200_': replace(
                        BPB_DOS_200_FAT12_EXAMPLE, total_size_200=4096
                    ),
                    'total_size_331': 8192,
                },
                r'Total size.*2\.0',
            ),
            (
                {
                    'bpb_dos_200_': replace(
                        BPB_DOS_200_FAT12_EXAMPLE, total_size_200=8192
                    ),
                    'total_size_331': 4096,
                },
                r'Total size.*2\.0',
            ),
        ],
    )
    def test_validate_fail(self, replace_kwargs, msg_contains):
        """Test custom validation logic for failing cases."""
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            replace(BPB_DOS_331_FAT16_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['volume_meta', 'replace_kwargs'],
        [
            (
                (0, BPB_DOS_331_FAT16_EXAMPLE.total_size_331 - 1, 512, 512),
                {'hidden_before_partition': 0},
            ),
            (
                (2048, BPB_DOS_331_FAT16_EXAMPLE.total_size_331 + 2047, 512, 512),
                {'hidden_before_partition': 2048},
            ),
            (
                (7, BPB_DOS_331_FAT16_EXAMPLE.total_size_331 + 6, 512, 512),
                {'hidden_before_partition': 7},
            ),
            ((0, 2047, 512, 512), {'total_size_331': 1024}),
            ((0, 2047, 512, 512), {'total_size_331': 2048}),
            ((0, 4095, 512, 512), {'total_size_331': 1001}),
            ((0, 99999, 512, 512), {'total_size_331': 100000}),
            ((0, 100999, 512, 512), {'total_size_331': 100000}),
        ],
        indirect=['volume_meta'],
    )
    def test_validate_for_volume_success(self, volume_meta, replace_kwargs):
        """Test validation against a specific volume for succeeding cases."""
        bpb = replace(BPB_DOS_331_FAT16_EXAMPLE, **replace_kwargs)
        bpb.validate_for_volume(volume_meta)

    @pytest.mark.parametrize(
        ['volume_meta', 'replace_kwargs', 'msg_contains'],
        [
            (
                (0, 2048, 4096, 4096),
                {
                    'bpb_dos_200_': replace(
                        BPB_DOS_200_FAT16_EXAMPLE, lss=512, rootdir_entries=16
                    )
                },
                'Logical sector size.*disk',
            ),
            ((0, 3, 512, 512), {'hidden_before_partition': 8}, 'Hidden sector'),
            ((0, 2047, 512, 512), {'hidden_before_partition': 2048}, 'Hidden sector'),
            ((0, 2047, 512, 512), {'total_size_331': 4096}, 'Total size.*volume'),
            ((0, 4095, 512, 512), {'total_size_331': 4097}, 'Total size.*volume'),
            ((0, 99999, 512, 512), {'total_size_331': 101000}, 'Total size.*volume'),
        ],
        indirect=['volume_meta'],
    )
    def test_validate_for_volume_fail(self, volume_meta, replace_kwargs, msg_contains):
        """Test validation against a specific volume for failing cases."""
        bpb = replace(BPB_DOS_331_FAT16_EXAMPLE, **replace_kwargs)
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            bpb.validate_for_volume(volume_meta)

    @pytest.mark.parametrize(
        ['bpb', 'total_size'],
        [
            (BPB_DOS_331_FAT12_EXAMPLE, 40960),
            (BPB_DOS_331_FAT16_EXAMPLE, 131072),
            (BPB_DOS_331_FAT32_EXAMPLE, 2097152),
            (replace(BPB_DOS_331_FAT32_EXAMPLE, total_size_331=0), None),
        ],
    )
    def test_properties(self, bpb, total_size):
        """Test that property values defined on BPBs match the expected values."""
        assert bpb.bpb_dos_200 is bpb.bpb_dos_200_
        assert bpb.total_size == total_size
        assert bpb.fat_size == bpb.bpb_dos_200_.fat_size_200


class TestShortEbpbFat:
    """Tests for ``ShortEbpbFat``."""

    @pytest.mark.parametrize(
        'replace_kwargs',
        [
            {
                'bpb_dos_331': replace(
                    BPB_DOS_331_FAT16_EXAMPLE,
                    bpb_dos_200_=replace(BPB_DOS_200_FAT16_EXAMPLE, lss=128),
                )
            },
            {
                'bpb_dos_331': replace(
                    BPB_DOS_331_FAT16_EXAMPLE,
                    bpb_dos_200_=replace(
                        BPB_DOS_200_FAT16_EXAMPLE, lss=128, rootdir_entries=4
                    ),
                )
            },
            {
                'bpb_dos_331': replace(
                    BPB_DOS_331_FAT16_EXAMPLE,
                    bpb_dos_200_=replace(BPB_DOS_200_FAT16_EXAMPLE, fat_size_200=1),
                )
            },
            {'physical_drive_number': 0x00},
            {'physical_drive_number': 0x40},
            {'physical_drive_number': 0x7E},
            {'physical_drive_number': 0xFE},
            {'reserved': 1},
            {'extended_boot_signature': b'\x28'},
        ],
    )
    def test_validate_success(self, replace_kwargs):
        """Test custom validation logic for succeeding cases."""
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            replace(SHORT_EBPB_FAT16_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['replace_kwargs', 'msg_contains'],
        [
            (
                {
                    'bpb_dos_331': replace(
                        BPB_DOS_331_FAT16_EXAMPLE,
                        bpb_dos_200_=replace(BPB_DOS_200_FAT16_EXAMPLE, lss=64),
                    )
                },
                'FAT requires a logical sector size',
            ),
            (
                {
                    'bpb_dos_331': replace(
                        BPB_DOS_331_FAT16_EXAMPLE,
                        bpb_dos_200_=replace(BPB_DOS_200_FAT16_EXAMPLE, lss=32),
                    )
                },
                'FAT requires a logical sector size',
            ),
            (
                {
                    'bpb_dos_331': replace(
                        BPB_DOS_331_FAT16_EXAMPLE,
                        bpb_dos_200_=replace(
                            BPB_DOS_200_FAT16_EXAMPLE, rootdir_entries=0
                        ),
                    )
                },
                'Root directory entry count',
            ),
            (
                {
                    'bpb_dos_331': replace(
                        BPB_DOS_331_FAT16_EXAMPLE,
                        bpb_dos_200_=replace(BPB_DOS_200_FAT16_EXAMPLE, fat_size_200=0),
                    )
                },
                r'FAT size.*2\.0',
            ),
            ({'extended_boot_signature': b'\x00'}, 'extended boot signature'),
            ({'extended_boot_signature': b'\x27'}, 'extended boot signature'),
            ({'extended_boot_signature': b'\x2A'}, 'extended boot signature'),
            ({'extended_boot_signature': b'\xFF'}, 'extended boot signature'),
        ],
    )
    def test_validate_fail(self, replace_kwargs, msg_contains):
        """Test custom validation logic for failing cases."""
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            replace(SHORT_EBPB_FAT16_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        'bpb', [SHORT_EBPB_FAT12_EXAMPLE, SHORT_EBPB_FAT16_EXAMPLE]
    )
    def test_properties(self, bpb):
        """Test that property values defined on BPBs match the expected values."""
        assert bpb.bpb_dos_200 is bpb.bpb_dos_331.bpb_dos_200_
        assert bpb.total_size == bpb.bpb_dos_331.total_size
        assert bpb.fat_size == bpb.bpb_dos_331.fat_size


class TestShortEbpbFat32:
    """Tests for ``ShortEbpbFat32``."""

    @pytest.mark.parametrize(
        'replace_kwargs',
        [
            {
                'bpb_dos_331': replace(
                    BPB_DOS_331_FAT32_EXAMPLE,
                    bpb_dos_200_=replace(BPB_DOS_200_FAT32_EXAMPLE, lss=1024),
                )
            },
            {'fat_size_32': 1},
            {'rootdir_start_cluster': 3},
            {'fsinfo_sector': 0},
            {'fsinfo_sector': 0xFFFF},
            {'reserved_1': b'\xFF' * 12},
            {'physical_drive_number': 0x00},
            {'physical_drive_number': 0x40},
            {'physical_drive_number': 0x7E},
            {'physical_drive_number': 0xFE},
            {'reserved_2': b'\xFF'},
            {'extended_boot_signature': b'\x28'},
        ],
    )
    def test_validate_success(self, replace_kwargs):
        """Test custom validation logic for succeeding cases."""
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            replace(SHORT_EBPB_FAT32_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['replace_kwargs', 'msg_contains'],
        [
            (
                {
                    'bpb_dos_331': replace(
                        BPB_DOS_331_FAT32_EXAMPLE,
                        bpb_dos_200_=replace(BPB_DOS_200_FAT32_EXAMPLE, lss=256),
                    )
                },
                'FAT32 requires a logical sector size',
            ),
            (
                {
                    'bpb_dos_331': replace(
                        BPB_DOS_331_FAT32_EXAMPLE,
                        bpb_dos_200_=replace(BPB_DOS_200_FAT32_EXAMPLE, lss=128),
                    )
                },
                'FAT32 requires a logical sector size',
            ),
            (
                {
                    'bpb_dos_331': replace(
                        BPB_DOS_331_FAT32_EXAMPLE,
                        bpb_dos_200_=replace(
                            BPB_DOS_200_FAT32_EXAMPLE,
                            rootdir_entries=ROOTDIR_ENTRIES_DEFAULT,
                        ),
                    )
                },
                'Root directory entry count',
            ),
            (
                {
                    'bpb_dos_331': replace(
                        BPB_DOS_331_FAT32_EXAMPLE,
                        bpb_dos_200_=replace(
                            BPB_DOS_200_FAT32_EXAMPLE, total_size_200=32768
                        ),
                        total_size_331=0,
                    )
                },
                r'Total size.*2\.0',
            ),
            (
                {
                    'bpb_dos_331': replace(
                        BPB_DOS_331_FAT32_EXAMPLE,
                        bpb_dos_200_=replace(BPB_DOS_200_FAT32_EXAMPLE, fat_size_200=1),
                    )
                },
                r'FAT size.*2\.0',
            ),
            ({'fat_size_32': 0}, 'FAT size'),
            ({'version': 1}, 'FAT32 version'),
            ({'rootdir_start_cluster': 0}, 'Root directory start cluster'),
            ({'rootdir_start_cluster': 1}, 'Root directory start cluster'),
            ({'fsinfo_sector': 2}, 'FS information sector number'),
            ({'extended_boot_signature': b'\x00'}, 'extended boot signature'),
            ({'extended_boot_signature': b'\x27'}, 'extended boot signature'),
            ({'extended_boot_signature': b'\x2A'}, 'extended boot signature'),
            ({'extended_boot_signature': b'\xFF'}, 'extended boot signature'),
        ],
    )
    def test_validate_fail(self, replace_kwargs, msg_contains):
        """Test custom validation logic for failing cases."""
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            replace(SHORT_EBPB_FAT32_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['fsinfo_available', 'backup_available', 'backup_start', 'reserved_size'],
        [
            (False, False, 0, 1),
            (False, False, 0, 2),
            (False, False, 0xFFFF, 1),
            (True, False, 0, 2),
            (True, False, 0, 3),
            (True, False, 0xFFFF, 2),
            (False, True, 1, 2),
            (False, True, 2, 4),
            (False, True, 3, 6),
            (True, True, 2, 4),
            (True, True, 3, 6),
        ],
    )
    def test_validate_reserved_sectors_success(
        self, fsinfo_available, backup_available, backup_start, reserved_size
    ):
        """Test that validation succeeds for valid combinations of values for reserved
        sector count, FS information sector and boot sector backup start sector.
        """
        bpb = replace(
            SHORT_EBPB_FAT32_EXAMPLE,
            bpb_dos_331=replace(
                BPB_DOS_331_FAT32_EXAMPLE,
                bpb_dos_200_=replace(
                    BPB_DOS_200_FAT32_EXAMPLE, reserved_size=reserved_size
                ),
            ),
            fsinfo_sector=int(fsinfo_available),
            boot_sector_backup_start=backup_start,
        )
        assert bpb.fsinfo_available is fsinfo_available
        assert bpb.backup_available is backup_available

    @pytest.mark.parametrize(
        ['fsinfo_available', 'backup_start', 'reserved_size', 'msg_contains'],
        [
            (True, 0, 1, 'Reserved sector count'),
            (True, 0xFFFF, 1, 'Reserved sector count'),
            (False, 1, 1, 'Reserved sector count'),
            (False, 2, 1, 'Reserved sector count'),
            (False, 2, 2, 'Reserved sector count'),
            (False, 2, 3, 'Reserved sector count'),
            (False, 3, 3, 'Reserved sector count'),
            (False, 3, 4, 'Reserved sector count'),
            (False, 3, 5, 'Reserved sector count'),
            (True, 1, 4, 'Boot sector backup start'),
            (True, 1, 6, 'Boot sector backup start'),
            (True, 2, 1, 'Reserved sector count'),
            (True, 2, 2, 'Reserved sector count'),
            (True, 2, 3, 'Reserved sector count'),
            (True, 3, 3, 'Reserved sector count'),
            (True, 3, 4, 'Reserved sector count'),
            (True, 3, 5, 'Reserved sector count'),
        ],
    )
    def test_validate_reserved_sectors_fail(
        self, fsinfo_available, backup_start, reserved_size, msg_contains
    ):
        """Test that validation fails for invalid combinations of values for reserved
        sector count, FS information sector and boot sector backup start sector.
        """
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            replace(
                SHORT_EBPB_FAT32_EXAMPLE,
                bpb_dos_331=replace(
                    BPB_DOS_331_FAT32_EXAMPLE,
                    bpb_dos_200_=replace(
                        BPB_DOS_200_FAT32_EXAMPLE, reserved_size=reserved_size
                    ),
                ),
                fsinfo_sector=int(fsinfo_available),
                boot_sector_backup_start=backup_start,
            )

    def test_properties(self):
        """Test that property values defined on BPBs match the expected values."""
        bpb = SHORT_EBPB_FAT32_EXAMPLE
        assert bpb.bpb_dos_200 is bpb.bpb_dos_331.bpb_dos_200_
        assert bpb.total_size == bpb.bpb_dos_331.total_size
        assert bpb.fat_size == bpb.fat_size_32

    @pytest.mark.parametrize(
        ['sector', 'available'], [(0, False), (1, True), (0xFFFF, False)]
    )
    def test_fsinfo_available(self, sector, available):
        bpb = replace(SHORT_EBPB_FAT32_EXAMPLE, fsinfo_sector=sector)
        assert bpb.fsinfo_available is available


class TestEbpbFat:
    """Tests for ``EbpbFat``."""

    @pytest.mark.parametrize(
        'replace_kwargs',
        [
            {'volume_id': 0},
            {'volume_label': b'DISKFS     '},
            {'file_system_type': b'FAT     '},
        ],
    )
    def test_validate_success(self, replace_kwargs):
        """Test custom validation logic for succeeding cases."""
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            replace(EBPB_FAT16_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['replace_kwargs', 'msg_contains'],
        [
            ({'file_system_type': b'FAT32   '}, 'file system type'),
            ({'file_system_type': b'NOTFAT  '}, 'file system type'),
        ],
    )
    def test_validate_warn(self, replace_kwargs, msg_contains):
        """Test custom validation logic for succeeding cases with warnings issued."""
        with pytest.warns(ValidationWarning, match=f'.*{msg_contains}.*'):
            replace(EBPB_FAT16_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['replace_kwargs', 'msg_contains'],
        [
            (
                {
                    'short': replace(
                        SHORT_EBPB_FAT16_EXAMPLE, extended_boot_signature=b'\x28'
                    )
                },
                'extended FAT EBPB',
            )
        ],
    )
    def test_validate_fail(self, replace_kwargs, msg_contains):
        """Test custom validation logic for failing cases."""
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            replace(EBPB_FAT16_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize('bpb', [EBPB_FAT12_EXAMPLE, EBPB_FAT16_EXAMPLE])
    def test_properties(self, bpb):
        """Test that property values defined on BPBs match the expected values."""
        assert bpb.bpb_dos_200 is bpb.short.bpb_dos_200
        assert bpb.total_size == bpb.short.total_size
        assert bpb.fat_size == bpb.short.fat_size


class TestEbpbFat32:
    """Tests for ``EbpbFat32``."""

    @pytest.mark.parametrize(
        'replace_kwargs', [{'volume_id': 1}, {'volume_label': b'DISKFS     '}]
    )
    def test_validate_success(self, replace_kwargs):
        """Test custom validation logic for succeeding cases."""
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            replace(EBPB_FAT32_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['replace_kwargs', 'msg_contains'],
        [
            ({'file_system_type': b'FAT     '}, 'file system type'),
            ({'file_system_type': b'FAT12   '}, 'file system type'),
            ({'file_system_type': b'FAT16   '}, 'file system type'),
            ({'file_system_type': b'NOTFAT  '}, 'file system type'),
            ({'file_system_type': b'NOTFAT32'}, 'file system type'),
        ],
    )
    def test_validate_warn(self, replace_kwargs, msg_contains):
        """Test custom validation logic for succeeding cases with warnings issued."""
        with pytest.warns(ValidationWarning, match=f'.*{msg_contains}.*'):
            replace(EBPB_FAT32_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['replace_kwargs', 'msg_contains'],
        [
            (
                {
                    'short': replace(
                        SHORT_EBPB_FAT32_EXAMPLE, extended_boot_signature=b'\x28'
                    )
                },
                'extended FAT32 EBPB',
            )
        ],
    )
    def test_validate_fail(self, replace_kwargs, msg_contains):
        """Test custom validation logic for failing cases."""
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            replace(EBPB_FAT32_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['volume_meta', 'replace_kwargs'],
        [
            (
                (0, (1 << 31) - 1, 512, 512),
                {
                    'short': replace(
                        SHORT_EBPB_FAT32_EXAMPLE,
                        bpb_dos_331=replace(
                            BPB_DOS_331_FAT32_EXAMPLE, total_size_331=(1 << 31)
                        ),
                    )
                },
            ),
            (
                (0, (1 << 31) - 1, 512, 512),
                {
                    'short': replace(
                        SHORT_EBPB_FAT32_EXAMPLE,
                        bpb_dos_331=replace(
                            BPB_DOS_331_FAT32_EXAMPLE, total_size_331=(1 << 31)
                        ),
                    ),
                    'file_system_type': (1 << 32).to_bytes(8, 'little'),
                },
            ),
            (
                (0, (1 << 31) - 1, 512, 512),
                {
                    'short': replace(
                        SHORT_EBPB_FAT32_EXAMPLE,
                        bpb_dos_331=replace(
                            BPB_DOS_331_FAT32_EXAMPLE, total_size_331=(1 << 31)
                        ),
                    ),
                    'file_system_type': b'\x00' * 8,
                },
            ),
            (
                (0, (1 << 64) - 2, 512, 512),
                {
                    'short': replace(
                        SHORT_EBPB_FAT32_EXAMPLE,
                        bpb_dos_331=replace(
                            BPB_DOS_331_FAT32_EXAMPLE, total_size_331=0
                        ),
                    ),
                    'file_system_type': b'\xFF' * 8,
                },
            ),
        ],
        indirect=['volume_meta'],
    )
    @pytest.mark.filterwarnings('ignore:Unknown file system type')
    def test_validate_for_volume_success(self, volume_meta, replace_kwargs):
        """Test validation against a specific volume for succeeding cases."""
        bpb = replace(EBPB_FAT32_EXAMPLE, **replace_kwargs)
        bpb.validate_for_volume(volume_meta)

    @pytest.mark.parametrize(
        ['volume_meta', 'replace_kwargs', 'msg_contains'],
        [
            (
                (0, (1 << 31) - 2, 512, 512),
                {
                    'short': replace(
                        SHORT_EBPB_FAT32_EXAMPLE,
                        bpb_dos_331=replace(
                            BPB_DOS_331_FAT32_EXAMPLE, total_size_331=(1 << 31)
                        ),
                    )
                },
                'Total size',
            ),
            (
                (0, (1 << 31) - 2, 512, 512),
                {
                    'short': replace(
                        SHORT_EBPB_FAT32_EXAMPLE,
                        bpb_dos_331=replace(
                            BPB_DOS_331_FAT32_EXAMPLE, total_size_331=(1 << 31)
                        ),
                    ),
                    'file_system_type': (1 << 20).to_bytes(8, 'little'),
                },
                'Total size',
            ),
            (
                (0, (1 << 64) - 3, 512, 512),
                {
                    'short': replace(
                        SHORT_EBPB_FAT32_EXAMPLE,
                        bpb_dos_331=replace(
                            BPB_DOS_331_FAT32_EXAMPLE, total_size_331=0
                        ),
                    ),
                    'file_system_type': b'\xFF' * 8,
                },
                'Total size',
            ),
        ],
        indirect=['volume_meta'],
    )
    @pytest.mark.filterwarnings('ignore:Unknown file system type')
    def test_validate_for_volume_fail(self, volume_meta, replace_kwargs, msg_contains):
        """Test validation against a specific volume for failing cases."""
        bpb = replace(EBPB_FAT32_EXAMPLE, **replace_kwargs)
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            bpb.validate_for_volume(volume_meta)

    @pytest.mark.parametrize(
        ['short', 'long', 'expected', 'warning_expected'],
        [
            (1 << 31, int.from_bytes(FILE_SYSTEM_TYPE_FAT32, 'little'), 1 << 31, False),
            (1 << 31, 1 << 16, 1 << 31, True),
            (1 << 31, 1 << 32, 1 << 31, True),
            (1 << 31, 0, 1 << 31, True),
            (0, 1 << 63, 1 << 63, True),
            (0, (1 << 64) - 1, (1 << 64) - 1, True),
            (0, 0, None, True),
        ],
    )
    @pytest.mark.filterwarnings('ignore:Unknown file system type')
    def test_total_size(self, short, long, expected, warning_expected):
        """Test that values for property ``total_size`` match the expected values."""
        bpb = replace(
            EBPB_FAT32_EXAMPLE,
            short=replace(
                SHORT_EBPB_FAT32_EXAMPLE,
                bpb_dos_331=replace(BPB_DOS_331_FAT32_EXAMPLE, total_size_331=short),
            ),
            file_system_type=long.to_bytes(8, 'little'),
        )
        assert bpb.total_size == expected

    def test_properties(self):
        """Test that property values defined on BPBs match the expected values."""
        bpb = EBPB_FAT32_EXAMPLE
        assert bpb.bpb_dos_200 is bpb.short.bpb_dos_200
        assert bpb.fat_size == bpb.short.fat_size
        assert bpb.fsinfo_available == bpb.short.fsinfo_available
        assert bpb.backup_available == bpb.short.backup_available


@pytest.mark.parametrize('physical_drive_number', [0x7F, 0xFF])
@pytest.mark.parametrize(
    'bpb',
    [SHORT_EBPB_FAT12_EXAMPLE, SHORT_EBPB_FAT16_EXAMPLE, SHORT_EBPB_FAT32_EXAMPLE],
)
def test_validate_warn_phyiscal_drive_number(bpb, physical_drive_number):
    """Test validation of short EBPBs for succeeding cases with warnings issued in
    case of reserved physical drive numbers.
    """
    with pytest.warns(ValidationWarning, match='.*physical drive number.*'):
        replace(bpb, physical_drive_number=physical_drive_number)


@pytest.mark.parametrize(
    ['volume_meta', 'bpb'],
    [
        ((0, bpb.total_size - 1, 512, 512), bpb)  # type: ignore[attr-defined]
        for bpb in (
            SHORT_EBPB_FAT12_EXAMPLE,
            SHORT_EBPB_FAT16_EXAMPLE,
            SHORT_EBPB_FAT32_EXAMPLE,
            EBPB_FAT12_EXAMPLE,
            EBPB_FAT16_EXAMPLE,
            EBPB_FAT32_EXAMPLE,
        )
    ],
    indirect=['volume_meta'],
)
def test_ebpb_validate_for_volume_success(volume_meta, bpb):
    """Test validation of EBPBs against a volume for specific succeeding cases common
    to all EBPBs.
    """
    bpb.validate_for_volume(volume_meta)


@pytest.mark.parametrize(
    ['volume_meta', 'bpb'],
    [
        ((0, bpb.total_size - 1, 4096, 4096), bpb)  # type: ignore[attr-defined]
        for bpb in (
            SHORT_EBPB_FAT12_EXAMPLE,
            SHORT_EBPB_FAT16_EXAMPLE,
            SHORT_EBPB_FAT32_EXAMPLE,
            EBPB_FAT12_EXAMPLE,
            EBPB_FAT16_EXAMPLE,
            EBPB_FAT32_EXAMPLE,
        )
    ],
    indirect=['volume_meta'],
)
def test_ebpb_validate_for_volume_fail_bpb_dos_200(volume_meta, bpb):
    """Test validation of EBPBs against a volume for a failing case caused by an
    invalid value in the encapsulated DOS 2.0 BPB common to all EBPBs.
    """
    short = getattr(bpb, 'short', bpb)
    short_new = replace(
        short,
        bpb_dos_331=replace(
            short.bpb_dos_331,
            bpb_dos_200_=replace(short.bpb_dos_331.bpb_dos_200_, lss=512),
        ),
    )
    bpb_new = short_new if bpb is short else replace(bpb, short=short_new)

    with pytest.raises(ValidationError, match='.*Logical sector size.*disk.*'):
        bpb_new.validate_for_volume(volume_meta)


@pytest.mark.parametrize(
    ['volume_meta', 'bpb'],
    [
        ((0, bpb.total_size - 1, 512, 512), bpb)  # type: ignore[attr-defined]
        for bpb in (
            SHORT_EBPB_FAT12_EXAMPLE,
            SHORT_EBPB_FAT16_EXAMPLE,
            SHORT_EBPB_FAT32_EXAMPLE,
            EBPB_FAT12_EXAMPLE,
            EBPB_FAT16_EXAMPLE,
            EBPB_FAT32_EXAMPLE,
        )
    ],
    indirect=['volume_meta'],
)
def test_ebpb_validate_for_volume_fail_bpb_dos_331(volume_meta, bpb):
    """Test validation of EBPBs against a volume for a failing case caused by an
    invalid value in the encapsulated DOS 3.31 BPB common to all EBPBs.
    """
    short = getattr(bpb, 'short', bpb)
    short_new = replace(
        short, bpb_dos_331=replace(short.bpb_dos_331, hidden_before_partition=1)
    )
    bpb_new = short_new if bpb is short else replace(bpb, short=short_new)

    with pytest.raises(ValidationError, match='.*Hidden sector.*'):
        bpb_new.validate_for_volume(volume_meta)
