"""Tests for the ``reserved`` module of the ``fat`` package."""

from dataclasses import replace

import pytest

from diskfs.base import SectorSize, ValidationError
from diskfs.fat.reserved import (
    CLUSTER_SIZE_DEFAULT,
    MEDIA_TYPE_DEFAULT,
    ROOTDIR_ENTRIES_DEFAULT,
    BpbDos200,
)
from diskfs.volume import Volume


@pytest.fixture
def volume_basic(request, monkeypatch):
    """Fixture providing a surface-level mocked instance of ``Volume`` with
    customizable ``start_lba``, ``end_lba`` and ``sector_size`` values.

    Parametrized using a ``tuple`` of the desired values for ``start_lba``,
    ``end_lba`` and ``sector_size``.
    """
    start, end, lss, pss = request.param
    size = end - start
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
    total_size_200=16384,  # 1 + 3 + 15 + 16365 sectors
    media_type=MEDIA_TYPE_DEFAULT,
    fat_size_200=3,
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
    rootdir_entries=0,
    total_size_200=0,
    media_type=MEDIA_TYPE_DEFAULT,
    fat_size_200=0,
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
            ((0, 2048, 512, 512), {'lss': 512, 'rootdir_entries': 16}),
            ((0, 2048, 512, 4096), {'lss': 512, 'rootdir_entries': 16}),
            ((0, 2048, 4096, 4096), {'lss': 4096, 'rootdir_entries': 128}),
            ((0, 2048, 512, 512), {'total_size_200': 1024}),
            ((0, 2048, 512, 512), {'total_size_200': 2048}),
            ((0, 4096, 512, 512), {'total_size_200': 2048}),
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
                (0, 2048, 4096, 512),
                {'lss': 512, 'rootdir_entries': 16},
                'Logical sector size.*disk',
            ),
            (
                (0, 2048, 512, 4096),
                {'lss': 4096, 'rootdir_entries': 128},
                'Logical sector size.*disk',
            ),
            ((0, 2048, 512, 512), {'total_size_200': 4096}, 'Total size'),
            ((0, 4096, 512, 512), {'total_size_200': 4097}, 'Total size'),
        ],
        indirect=['volume_basic'],
    )
    def test_validate_for_volume_fail(self, volume_basic, replace_kwargs, msg_contains):
        """Test validation against a specific volume for failing cases."""
        bpb = replace(BPB_DOS_200_FAT16_EXAMPLE, **replace_kwargs)
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            bpb.validate_for_volume(volume_basic)

    @pytest.mark.parametrize(
        ['bpb', 'expected_total_size'],
        [
            (BPB_DOS_200_FAT12_EXAMPLE, 16384),
            (BPB_DOS_200_FAT16_EXAMPLE, None),
            (BPB_DOS_200_FAT32_EXAMPLE, None),
        ],
    )
    def test_properties(self, bpb, expected_total_size):
        """Test that property values defined on BPBs match the expected values."""
        assert bpb.bpb_dos_200 is bpb
        assert bpb.fat_size == bpb.fat_size_200
        assert bpb.total_size == expected_total_size
