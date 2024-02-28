"""Tests for the ``base`` module."""

import pytest

from diskfs.base import is_power_of_two


@pytest.mark.parametrize(
    "value", [1, 2, 4, 8, 16, 32, 64, 128, 512, 1024, 2048, 4096, 1048576]
)
def test_is_power_of_two_positive(value):
    """Test successful positive evaluation of ``is_power_of_two()``."""
    assert is_power_of_two(value)


@pytest.mark.parametrize("value", [3, 5, 6, 7, 9, 10, 12, 20, 24, 63, 384, 1000])
def test_is_power_of_two_negative(value):
    """Test successful negative evaluation of ``is_power_of_two()``."""
    assert not is_power_of_two(value)


@pytest.mark.parametrize("value", [0, -1, -4, -7, -256])
def test_is_power_of_two_fail(value):
    """Test ``is_power_of_two()`` against parameters ``value`` which are expected to
    fail.
    """
    with pytest.raises(ValueError):
        is_power_of_two(value)
