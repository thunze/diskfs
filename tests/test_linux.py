"""Tests for the ``linux`` module."""

import sys
from typing import TYPE_CHECKING

import pytest

# Make mypy and pytest collection logic happy
if not TYPE_CHECKING and sys.platform != 'linux':
    pytest.skip('Skipping Linux-only tests', allow_module_level=True)
assert sys.platform == 'linux'


from diskfs.base import SectorSize
from diskfs.linux import reread_partition_table


@pytest.mark.privileged
@pytest.mark.parametrize('block_device', [(8192, SectorSize(512, 4096))], indirect=True)
def test_reread_partition_table(block_device):
    """Test that correctly invoking ``reread_partition_table()`` does not raise an
    exception.
    """
    with open(block_device, 'rb') as f:
        reread_partition_table(f)
