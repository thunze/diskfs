"""Tests for the ``reserved`` module of the ``fat`` package."""

from dataclasses import replace

import pytest

from diskfs.base import SectorSize, ValidationError, ValidationWarning
from diskfs.fat.reserved import (
    CLUSTER_SIZE_DEFAULT,
    EXTENDED_BOOT_SIGNATURE_EXISTS,
    HEADS_DEFAULT,
    MEDIA_TYPE_DEFAULT,
    PHYSICAL_DRIVE_NUMBER_DEFAULT,
    ROOTDIR_ENTRIES_DEFAULT,
    SECTORS_PER_TRACK_DEFAULT,
    BpbDos200,
    BpbDos331,
    ShortEbpbFat,
)
from diskfs.volume import Volume


@pytest.fixture
def volume_basic(request, monkeypatch):
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


# TODO: Test BPBs used for formatting

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
        ['volume_basic', 'replace_kwargs'],
        [
            ((0, 2047, 512, 512), {'lss': 512, 'rootdir_entries': 16}),
            ((0, 2047, 512, 4096), {'lss': 512, 'rootdir_entries': 16}),
            ((0, 2047, 4096, 4096), {'lss': 4096, 'rootdir_entries': 128}),
            ((0, 2047, 512, 512), {'total_size_200': 1024}),
            ((0, 2047, 512, 512), {'total_size_200': 2048}),
            ((0, 4095, 512, 512), {'total_size_200': 1001}),
        ],
        indirect=['volume_basic'],
    )
    def test_validate_for_volume_success(self, volume_basic, replace_kwargs):
        """Test validation against a specific volume for succeeding cases."""
        bpb = replace(BPB_DOS_200_FAT16_EXAMPLE, **replace_kwargs)
        bpb.validate_for_volume(volume_basic)

    @pytest.mark.parametrize(
        ['volume_basic', 'replace_kwargs', 'msg_contains'],
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
        indirect=['volume_basic'],
    )
    def test_validate_for_volume_fail(self, volume_basic, replace_kwargs, msg_contains):
        """Test validation against a specific volume for failing cases."""
        bpb = replace(BPB_DOS_200_FAT16_EXAMPLE, **replace_kwargs)
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            bpb.validate_for_volume(volume_basic)

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
        ['volume_basic', 'replace_kwargs'],
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
        indirect=['volume_basic'],
    )
    def test_validate_for_volume_success(self, volume_basic, replace_kwargs):
        """Test validation against a specific volume for succeeding cases."""
        bpb = replace(BPB_DOS_331_FAT16_EXAMPLE, **replace_kwargs)
        bpb.validate_for_volume(volume_basic)

    @pytest.mark.parametrize(
        ['volume_basic', 'replace_kwargs', 'msg_contains'],
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
        indirect=['volume_basic'],
    )
    def test_validate_for_volume_fail(self, volume_basic, replace_kwargs, msg_contains):
        """Test validation against a specific volume for failing cases."""
        bpb = replace(BPB_DOS_331_FAT16_EXAMPLE, **replace_kwargs)
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            bpb.validate_for_volume(volume_basic)

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
        replace(SHORT_EBPB_FAT16_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['replace_kwargs', 'msg_contains'],
        [
            ({'physical_drive_number': 0x7F}, 'physical drive number'),
            ({'physical_drive_number': 0xFF}, 'physical drive number'),
        ],
    )
    def test_validate_warn(self, replace_kwargs, msg_contains):
        """Test custom validation logic for succeeding cases with warnings issued."""
        with pytest.warns(ValidationWarning, match=f'.*{msg_contains}.*'):
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
        'volume_basic',
        [(0, SHORT_EBPB_FAT16_EXAMPLE.bpb_dos_331.total_size_331 - 1, 512, 512)],
        indirect=['volume_basic'],
    )
    def test_validate_for_volume_success(self, volume_basic):
        """Test validation against a specific volume for succeeding cases."""
        SHORT_EBPB_FAT16_EXAMPLE.validate_for_volume(volume_basic)

    @pytest.mark.parametrize(
        ['volume_basic', 'replace_kwargs', 'msg_contains'],
        [
            (
                (
                    0,
                    SHORT_EBPB_FAT16_EXAMPLE.bpb_dos_331.total_size_331 - 1,
                    4096,
                    4096,
                ),
                {
                    'bpb_dos_331': replace(
                        BPB_DOS_331_FAT16_EXAMPLE,
                        bpb_dos_200_=replace(
                            BPB_DOS_200_FAT16_EXAMPLE, lss=512, rootdir_entries=16
                        ),
                    )
                },
                'Logical sector size.*disk',
            ),
            (
                (0, SHORT_EBPB_FAT16_EXAMPLE.bpb_dos_331.total_size_331 - 1, 512, 512),
                {
                    'bpb_dos_331': replace(
                        BPB_DOS_331_FAT16_EXAMPLE, hidden_before_partition=1
                    )
                },
                'Hidden sector',
            ),
        ],
        indirect=['volume_basic'],
    )
    def test_validate_for_volume_fail(self, volume_basic, replace_kwargs, msg_contains):
        """Test validation against a specific volume for failing cases."""
        bpb = replace(SHORT_EBPB_FAT16_EXAMPLE, **replace_kwargs)
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            bpb.validate_for_volume(volume_basic)

    @pytest.mark.parametrize(
        'bpb', [SHORT_EBPB_FAT12_EXAMPLE, SHORT_EBPB_FAT16_EXAMPLE]
    )
    def test_properties(self, bpb):
        """Test that property values defined on BPBs match the expected values."""
        assert bpb.bpb_dos_200 is bpb.bpb_dos_331.bpb_dos_200_
        assert bpb.total_size == bpb.bpb_dos_331.total_size
        assert bpb.fat_size == bpb.bpb_dos_331.fat_size
