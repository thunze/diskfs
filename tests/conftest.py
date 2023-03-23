"""Fixtures used across the test suite."""

import os
import subprocess
import sys
from pathlib import Path
from shutil import copyfileobj, rmtree
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

    import gzip
    import importlib.resources

    from . import data

    @pytest.fixture
    def block_device(request, tempfile):
        size, (lss, pss) = request.param
        gzipped_filename = f'empty_{size}_{lss}_{pss}.vhdx.gz'

        # Decompress VHDX file
        with importlib.resources.path(data, gzipped_filename) as gzipped_path:
            with gzip.open(gzipped_path, 'rb') as f_in:
                with tempfile.open('wb') as f_out:
                    copyfileobj(f_in, f_out)

        # Mount virtual hard disk
        backfile_path = tempfile.absolute()
        mount_command = (
            f'(Mount-DiskImage "{backfile_path}" -NoDriveLetter -StorageType VHDX)'
            f'.DevicePath'
        )
        completed_process = subprocess.run(
            ['powershell.exe', '-Command', mount_command],
            capture_output=True,
            check=True,
            encoding='utf-8',
        )
        device_path = completed_process.stdout.rstrip()
        yield device_path

        # Clean up
        dismount_command = f'Dismount-DiskImage -DevicePath "{device_path}"'
        subprocess.run(['powershell.exe', '-Command', dismount_command])

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
        yield device_path

        # Clean up
        subprocess.run(['losetup', '-d', device_path])

elif sys.platform == 'darwin':

    @pytest.fixture
    def block_device(request, tempfile):
        size, _ = request.param

        # Expand new temporary file to desired size
        with tempfile.open('wb') as f:
            f.truncate(size)

        # Attach disk image
        backfile_path = tempfile.absolute()
        completed_process = subprocess.run(
            [
                'hdiutil',
                'attach',
                '-imagekey',
                'diskimage-class=CRawDiskImage',
                '-nomount',
                backfile_path,
            ],
            capture_output=True,
            check=True,
            encoding='utf-8',
        )
        device_path = completed_process.stdout.split()[0]  # see man hdiutil
        yield device_path

        # Clean up
        subprocess.run(['hdiutil', 'detach', device_path])

else:
    raise RuntimeError(f'Unspported platform {sys.platform!r}')


block_device.__doc__ = """Fixture providing a virtual block device for testing purposes.

Parametrized using a ``tuple`` of (desired size of the block device, desired
sector size pair for the block device). Some platforms do not support setting the
logical or physical sector size for a virtual block device. In that case,
values which cannot be used are ignored.

Returns a string representing the path of the block device.
"""
