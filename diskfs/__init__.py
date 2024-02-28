"""Disk and file system manipulation.

Many concepts based on ``go-diskfs`` (see https://github.com/diskfs/go-diskfs).
"""

from .disk import Disk

__all__ = ["Disk"]
