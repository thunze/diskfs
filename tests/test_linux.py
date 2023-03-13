"""Tests for the ``linux`` module."""

import sys
from typing import TYPE_CHECKING

import pytest

# Make mypy and pytest collection happy
if not TYPE_CHECKING and sys.platform != 'linux':
    pytest.skip('Skipping Linux-only tests', allow_module_level=True)
assert sys.platform == 'linux'

import os
import subprocess
from pathlib import Path
from tempfile import mkstemp

from diskfs.base import is_power_of_two
from diskfs.linux import device_sector_size, device_size, reread_partition_table


@pytest.fixture(scope='module')
def tempfile():
    """Fixture providing a new temporary file for testing purposes.

    Returns a ``pathlib.Path`` object representing the path of the temporary file.
    """
    fd, path_str = mkstemp()
    os.close(fd)  # we are going to use a Path object instead
    path = Path(path_str)
    yield path
    path.unlink()  # clean up


@pytest.fixture(scope='module')
def block_device(request, tempfile):
    """Fixture providing a virtual block device for testing purposes.

    It is supposed to be parametrized using a ``tuple`` of (desired size of the block
    device, desired logical sector size of the block device).

    Returns a ``pathlib.Path`` object representing the path of the block device.
    """
    size, lss = request.param

    # Expand new temporary file to desired size
    with tempfile.open('wb') as f:
        f.truncate(size)

    # Create loop device
    backfile_path = tempfile.absolute()
    completed_process = subprocess.run(
        ['losetup', '-fLP', '-b', str(lss), '--show', backfile_path],
        capture_output=True,
        check=True,
        encoding='utf-8',
    )
    device_path = completed_process.stdout.rstrip()
    yield Path(device_path)

    # Clean up
    subprocess.run(['losetup', '-d', device_path])


@pytest.mark.privileged
@pytest.mark.parametrize(
    ['block_device', 'size'],
    [((size, 512), size) for size in [512, 1024, 8192]],
    indirect=['block_device'],
)
def test_device_size(block_device, size):
    """Test that ``device_size()`` returns the size of a block device."""
    with block_device.open('rb') as f:
        assert device_size(f) == size


@pytest.mark.privileged
@pytest.mark.parametrize(
    ['block_device', 'lss'],
    [((8192, lss), lss) for lss in [512, 2048, 4096]],
    indirect=['block_device'],
)
def test_device_sector_size(block_device, lss):
    """Test that ``device_sector_size()`` returns the logical sector size of a block
    device.
    """
    with block_device.open('rb') as f:
        sector_size = device_sector_size(f)

    assert sector_size.logical == lss

    # We cannot customize the PSS using losetup, so let's at least test that it's
    # a power of two greater than or equal to the LSS.
    assert is_power_of_two(sector_size.physical)
    assert sector_size.physical >= lss


@pytest.mark.privileged
@pytest.mark.parametrize('block_device', [(1024, 512), (34816, 512)], indirect=True)
def test_reread_partition_table(block_device):
    """Test that correctly invoking ``reread_partition_table()`` does not raise an
    exception.
    """
    with block_device.open('rb') as f:
        reread_partition_table(f)
