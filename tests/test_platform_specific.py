"""Generalized tests for the platform-specific modules."""

import sys

import pytest

from diskfs.base import DeviceProperties, SectorSize, is_power_of_two

# Test logic is the same, only parametrization has to be adjusted per platform.

if sys.platform == 'win32':
    from diskfs import win32 as platform_specific

    # 3 MiB is minimum required by New-VHD
    SIZES = [3 * 1024 * 1024, 4 * 1024 * 1024]

    # Only combinations allowed by New-VHD
    SECTOR_SIZES = [SectorSize(512, 512), SectorSize(512, 4096), SectorSize(4096, 4096)]
    SECTOR_SIZE_CUSTOMIZABLE = (True, True)  # LSS and PSS customizable
    DEVICE_PROPERTIES = DeviceProperties(False, None, None)

elif sys.platform == 'linux':
    from diskfs import linux as platform_specific

    SIZES = [2 * 4096, 3 * 4096]
    SECTOR_SIZES = [
        SectorSize(512, 4096),
        SectorSize(2048, 4096),
        SectorSize(4096, 4096),
    ]
    SECTOR_SIZE_CUSTOMIZABLE = (True, False)  # only LSS customizable
    DEVICE_PROPERTIES = DeviceProperties(False, None, None)

elif sys.platform == 'darwin':
    from diskfs import darwin as platform_specific

    SIZES = [2 * 4096, 3 * 4096]
    SECTOR_SIZES = [SectorSize(512, 512)]
    SECTOR_SIZE_CUSTOMIZABLE = (False, False)  # not customizable at all
    DEVICE_PROPERTIES = DeviceProperties(True, 'Apple', 'Disk Image')

else:
    raise RuntimeError(f'Unspported platform {sys.platform!r}')


# Test logic

SECTOR_SIZE_MIN_SANE = 128
SECTOR_SIZE_MAX_SANE = 16 * 1024 * 1024


@pytest.mark.privileged
@pytest.mark.parametrize('block_device', [(SIZES[0], SECTOR_SIZES[0])], indirect=True)
def test_device_properties(block_device) -> None:
    """Test that the ``DeviceProperties`` tuple obtained from ``device_properties()``
    contains the expected values.
    """
    with open(block_device, 'rb') as f:
        assert platform_specific.device_properties(f) == DEVICE_PROPERTIES


@pytest.mark.privileged
@pytest.mark.parametrize(
    ['block_device', 'size_expected', 'sector_size_expected'],
    [
        ((size, sector_size), size, sector_size)  # pass to fixture and to test function
        for size in SIZES
        for sector_size in SECTOR_SIZES
    ],
    indirect=['block_device'],
)
def test_device_size_sector_size(
    block_device, size_expected, sector_size_expected
) -> None:
    """Test that ``device_size()`` and ``device_sector_size()`` return the expected
    values.
    """
    with open(block_device, 'rb') as f:
        size_actual = platform_specific.device_size(f)
        sector_size_actual = platform_specific.device_sector_size(f)

    # Check size
    assert size_actual == size_expected

    # Check sector size
    for actual, expected, customizable in zip(
        sector_size_actual, sector_size_expected, SECTOR_SIZE_CUSTOMIZABLE
    ):
        if customizable:
            assert actual == expected
        else:
            # If we don't know the expected LSS/PSS, the best we can do is check that
            # the queried value is a power of two roughly lying within a sane range.
            assert is_power_of_two(actual)
            assert SECTOR_SIZE_MIN_SANE <= actual <= SECTOR_SIZE_MAX_SANE

    assert sector_size_actual.physical >= sector_size_actual.logical


@pytest.mark.privileged
@pytest.mark.parametrize('block_device', [(SIZES[0], SECTOR_SIZES[0])], indirect=True)
def test_reread_partition_table(block_device):
    """Test that correctly invoking ``reread_partition_table()`` does not raise an
    exception.
    """
    with open(block_device, 'rb') as f:
        platform_specific.reread_partition_table(f)
