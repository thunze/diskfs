"""Wrapper module for types, functions and constants exported by Darwin libraries."""

from ctypes import CDLL, c_bool, c_char_p, c_int, c_uint32, c_void_p
from pathlib import Path
from typing import NewType

__all__ = [
    # Types
    "CFIndex",
    "CFStringEncoding",
    "CFAllocator",
    "CFBoolean",
    "CFDictionary",
    "CFString",
    "DADisk",
    "DASession",
    # Functions
    "CFBooleanGetValue",
    "CFDictionaryGetValue",
    "CFRelease",
    "CFStringGetCString",
    "CFStringGetLength",
    "CFStringGetMaximumSizeForEncoding",
    "DADiskCopyDescription",
    "DADiskCreateFromBSDName",
    "DASessionCreate",
    # Constants
    "ENCODING_UTF_8",
    "MODEL_KEY",
    "REMOVABLE_KEY",
    "VENDOR_KEY",
]


# Types available at runtime

CFIndex = int
CFStringEncoding = int

CFTypeRef = NewType("CFTypeRef", c_void_p)
CFAllocator = NewType("CFAllocator", CFTypeRef)
CFBoolean = NewType("CFBoolean", CFTypeRef)
CFDictionary = NewType("CFDictionary", CFTypeRef)
CFString = NewType("CFString", CFTypeRef)
DADisk = NewType("DADisk", CFTypeRef)
DASession = NewType("DASession", CFTypeRef)


# Functions and constants

FRAMEWORK_BASE = Path("/System/Library/Frameworks")


def framework(name: str) -> CDLL:
    """Return a `ctypes` shared library object for the framework available under the
    name `name`.
    """
    path = FRAMEWORK_BASE / f"{name}.framework" / name
    return CDLL(str(path))


CoreFoundation = framework("CoreFoundation")
DiskArbitration = framework("DiskArbitration")


ENCODING_UTF_8 = 134217984  # value of enum CFStringBuiltInEncodings
REMOVABLE_KEY = c_void_p.in_dll(DiskArbitration, "kDADiskDescriptionMediaRemovableKey")
VENDOR_KEY = c_void_p.in_dll(DiskArbitration, "kDADiskDescriptionDeviceVendorKey")
MODEL_KEY = c_void_p.in_dll(DiskArbitration, "kDADiskDescriptionDeviceModelKey")


CFBooleanGetValue = CoreFoundation.CFBooleanGetValue
CFBooleanGetValue.argtypes = [c_void_p]
CFBooleanGetValue.restype = c_bool

CFDictionaryGetValue = CoreFoundation.CFDictionaryGetValue
CFDictionaryGetValue.argtypes = [c_void_p, c_void_p]
CFDictionaryGetValue.restype = c_void_p

CFRelease = CoreFoundation.CFRelease
CFRelease.argtypes = [c_void_p]
CFRelease.restype = None

CFStringGetCString = CoreFoundation.CFStringGetCString
CFStringGetCString.argtypes = [c_void_p, c_char_p, c_int, c_uint32]
CFStringGetCString.restype = c_bool

CFStringGetLength = CoreFoundation.CFStringGetLength
CFStringGetLength.argtypes = [c_void_p]
CFStringGetLength.restype = c_int

CFStringGetMaximumSizeForEncoding = CoreFoundation.CFStringGetMaximumSizeForEncoding
CFStringGetMaximumSizeForEncoding.argtypes = [c_int, c_uint32]
CFStringGetMaximumSizeForEncoding.restype = c_int

DADiskCopyDescription = DiskArbitration.DADiskCopyDescription
DADiskCopyDescription.argtypes = [c_void_p]
DADiskCopyDescription.restype = c_void_p

DADiskCreateFromBSDName = DiskArbitration.DADiskCreateFromBSDName
DADiskCreateFromBSDName.argtypes = [c_void_p, c_void_p, c_char_p]
DADiskCreateFromBSDName.restype = c_void_p

DASessionCreate = DiskArbitration.DASessionCreate
DASessionCreate.argtypes = [c_void_p]
DASessionCreate.restype = c_void_p
