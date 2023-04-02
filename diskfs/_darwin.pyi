"""Stub for the ``_darwin`` module."""

# noinspection PyUnresolvedReferences, PyProtectedMember
from ctypes import _CData, c_void_p
from typing import NewType

Buffer = bytes | _CData | None  # c_char_p
CFIndex = int  # c_int
CFStringEncoding = int  # c_uint32

CFTypeRef = NewType('CFTypeRef', c_void_p)  # explicitly not None
CFAllocator = NewType('CFAllocator', CFTypeRef)
CFBoolean = NewType('CFBoolean', CFTypeRef)
CFDictionary = NewType('CFDictionary', CFTypeRef)
CFString = NewType('CFString', CFTypeRef)
DADisk = NewType('DADisk', CFTypeRef)
DASession = NewType('DASession', CFTypeRef)

ENCODING_UTF_8: CFStringEncoding
REMOVABLE_KEY: CFString
VENDOR_KEY: CFString
MODEL_KEY: CFString

def CFBooleanGetValue(boolean: CFBoolean) -> bool: ...
def CFDictionaryGetValue(
    dictionary: CFDictionary, key: CFString
) -> CFTypeRef | None: ...
def CFRelease(cf: CFTypeRef) -> None: ...
def CFStringGetCString(
    string: CFString, buffer: Buffer, buffer_size: CFIndex, encoding: CFStringEncoding
) -> bool: ...
def CFStringGetLength(string: CFString) -> CFIndex: ...
def CFStringGetMaximumSizeForEncoding(
    length: CFIndex, encoding: CFStringEncoding
) -> CFIndex: ...
def DADiskCopyDescription(disk: DADisk) -> CFDictionary | None: ...
def DADiskCreateFromBSDName(
    allocator: CFAllocator | None, session: DASession, name: Buffer
) -> DADisk | None: ...
def DASessionCreate(allocator: CFAllocator | None) -> DASession | None: ...
