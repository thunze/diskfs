"""Fixtures used across the test suite."""

import os
import subprocess
import sys
from pathlib import Path
from shutil import rmtree
from tempfile import mkdtemp, mkstemp

import pytest


@pytest.fixture
def tempdir():
    """Fixture providing a new temporary directory for testing purposes.

    Returns a ``pathlib.Path`` object representing the path of the temporary directory.
    """
    path = Path(mkdtemp())
    yield path
    rmtree(path)  # clean up


@pytest.fixture
def tempfile():
    """Fixture providing a new temporary file for testing purposes.

    Returns a ``pathlib.Path`` object representing the path of the temporary file.
    """
    fd, path_str = mkstemp()
    os.close(fd)  # we are going to use a Path object instead
    path = Path(path_str)
    yield path
    path.unlink(missing_ok=True)  # clean up


# Platform-specific implementation

if sys.platform == 'win32':

    @pytest.fixture
    def block_device(request, tempdir):
        # We are using a temporary directory here because a temporary file would have
        # to be deleted first in order for New-VHD to function. However, this would
        # impose a race condition as we cannot guarantee the filename to stay unused.
        size, (lss, pss) = request.param

        # Create and mount virtual hard disk
        backfile_path = (tempdir / 'temp.vhdx').absolute()
        command = (
            f'(New-VHD -Path "{backfile_path}" -SizeBytes {size} -Dynamic '
            f'-LogicalSectorSizeBytes {lss} -PhysicalSectorSizeBytes {pss} '
            f'| Mount-VHD -Passthru | Get-Disk).Path'
        )
        completed_process = subprocess.run(
            ['powershell.exe', '-Command', command],
            capture_output=True,
            encoding='utf-8',
        )
        device_path = completed_process.stdout.rstrip()
        yield Path(device_path)

        # Clean up
        subprocess.run(
            ['powershell.exe', '-Command', f'Dismount-VHD "{backfile_path}"']
        )

elif sys.platform == 'linux':

    @pytest.fixture
    def block_device(request, tempfile):
        size, (lss, pss) = request.param

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

elif sys.platform == 'darwin':

    @pytest.fixture
    def block_device(request, tempfile):
        size, (lss, pss) = request.param
        raise NotImplementedError

else:
    raise RuntimeError(f'Unspported platform {sys.platform!r}')


block_device.__doc__ = """Fixture providing a virtual block device for testing purposes.

Parametrized using a ``tuple`` of (desired size of the block device, desired
sector size pair for the block device). Some platforms do not support setting the
logical or physical sector size for a virtual block device. In that case,
values which cannot be used are ignored.

Returns a ``pathlib.Path`` object representing the path of the block device.
"""
