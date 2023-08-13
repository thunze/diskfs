"""Directory and directory entry."""

from __future__ import annotations

import logging
from ctypes import c_int32, c_int64, c_uint8, c_uint16, c_uint64
from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum, Flag
from typing import Any, Collection, Iterable, Iterator, Literal, overload

from typing_extensions import Annotated

from ..base import ValidationError
from ..bytestruct import ByteStruct
from ..filesystem import FileSystemLimit

__all__ = [
    'ENTRY_SIZE',
    'Attributes',
    'Hint',
    'Entry',
    'EightDotThreeEntry',
    'entry_match',
    'iter_entries',
    'create_entry',
    'updated_entry',
    'pack_dos_datetime',
    'unpack_dos_datetime',
]


log = logging.getLogger(__name__)


ENTRY_SIZE = 32
MAX_VFAT_ENTRIES = 20

DOS_FILENAME_OEM_ENCODING = '850'
"""OEM encoding used for DOS filenames.

DOS filenames support characters < 256, but the encoding of characters >= 128 depends
on the file system driver. We cannot know in advance which encoding was used to
encode filenames on the file system, so we go for code page 850 as the default. This
constant is meant to be changed if needed but it should only be changed to names of
8-bit 1-byte encodings.
On Windows, you can find out the active code page by executing the command ``chcp``
or by calling ``GetConsoleOutputCP()`` found in ``kernel32.dll``.
"""

# Applies to already unpacked DOS filenames.
# Implicitly includes characters already found in VFAT_FILENAME_FORBIDDEN and all
# lowercase characters (depending on the OEM code page).
DOS_FILENAME_FORBIDDEN = '+,.;=[]'

VFAT_FILENAME_MAX_LENGTH = 255
VFAT_FILENAME_FORBIDDEN = ''.join(map(chr, range(32))) + '"*/:<>?\\|\x7F'
VFAT_FIRST_LFN_ENTRY = 0b0100_0000
VFAT_ENTRY_NUMBER_MASK = 0b0001_1111

ACTUALLY_E5 = 0x05
CASE_INFO_NAME_LOWER = 0b1000
CASE_INFO_EXT_LOWER = 0b10000
DOS_YEAR_MIN = 1980
DOS_YEAR_MAX = 2107
DOS_TIME_TEN_MS_MAX = 199


class Hint(Enum):
    """Possible special meanings of the first character of a short filename.

    This excludes 0x05 (meaning that the first character of the short filename is
    actually 0xE5) because it only needs to be handled in filename packing and
    unpacking.
    """

    END_OF_ENTRIES = 0x00
    DOT_ENTRY = 0x2E
    DELETED = 0xE5


class Attributes(Flag):
    """Directory entry attributes."""

    READ_ONLY = 1 << 0
    HIDDEN = 1 << 1
    SYSTEM = 1 << 2
    VOLUME_LABEL = 1 << 3
    SUBDIRECTORY = 1 << 4
    ARCHIVE = 1 << 5
    DEVICE = 1 << 6
    RESERVED = 1 << 7

    VFAT = READ_ONLY | HIDDEN | SYSTEM | VOLUME_LABEL


def _split_filename(filename: str) -> tuple[str, str]:
    """Split ``filename`` into name and (rightmost) extension.

    Examples:
        - 'thing.json' -> ('thing', 'json')
        - 'thing.json.txt' -> ('thing.json', 'txt')
        - 'thing' -> ('thing', '')
    """
    split_ = filename.rsplit('.', maxsplit=1)
    assert len(split_) <= 2
    if len(split_) == 1:
        return split_[0], ''
    return split_[0], split_[1]


def _pack_dos_filename(filename: str) -> tuple[bytes, bytes]:
    """Pack a DOS filename into a name of 8 bytes and an extension of 3 bytes.

    Assumption: Length and validity of the filename have already been checked.
    """
    name_str, ext_str = _split_filename(filename)
    name_bytes = name_str.encode(DOS_FILENAME_OEM_ENCODING)
    ext_bytes = ext_str.encode(DOS_FILENAME_OEM_ENCODING)

    # Pad with spaces
    packed_name = name_bytes.ljust(8)
    packed_ext = ext_bytes.ljust(3)
    if packed_name[0] == Hint.DELETED.value:
        packed_name = bytes([ACTUALLY_E5]) + packed_name[1:]
    return packed_name, packed_ext


def _unpack_dos_filename(name_bytes: bytes, ext_bytes: bytes) -> str:
    """Unpack a DOS filename from a name of 8 bytes and an extension of 3 bytes.

    Characters which cannot be decoded using ``DOS_FILENAME_OEM_ENCODING`` are
    replaced by the Unicode replacement character ``U+FFFD``.
    """
    unpacked_name = name_bytes.rstrip(b' ')
    unpacked_ext = ext_bytes.rstrip(b' ')
    if unpacked_name[0] == ACTUALLY_E5:
        unpacked_name = bytes([Hint.DELETED.value]) + unpacked_name[1:]

    name_str = unpacked_name.decode(DOS_FILENAME_OEM_ENCODING, errors='replace')
    ext_str = unpacked_ext.decode(DOS_FILENAME_OEM_ENCODING, errors='replace')
    if ext_str:
        return f'{name_str}.{ext_str}'
    return name_str


def _is_invalid_dos_character(char: str) -> bool:
    """Return whether ``char`` is a character not allowed in DOS filenames.

    Characters already prohibited by general FAT filename rules are not considered by
    this check (see ``_is_valid_vfat_filename()``).
    """
    try:
        char.encode(DOS_FILENAME_OEM_ENCODING)
    except UnicodeEncodeError:
        return True

    # The islower() check is necessary to rule out any potential lowercase characters
    # specific to the OEM encoding (codepoints 128-255) in addition to [a-z].
    return char in DOS_FILENAME_FORBIDDEN or char.islower()


def _has_invalid_dos_character(filename: str) -> bool:
    """Return whether ``filename`` contains at least one character not allowed in DOS
    filenames.

    Characters already prohibited by general FAT filename rules are not considered by
    this check (see ``_is_valid_vfat_filename()``).
    """
    for part in _split_filename(filename):
        if any(_is_invalid_dos_character(char) for char in part):
            return True
    return False


def _is_valid_dos_filename(filename: str) -> bool:
    """Return whether ``filename`` is a valid DOS filename."""
    name, ext = _split_filename(filename)
    return (
        _is_valid_vfat_filename(filename)
        and len(name) <= 8
        and len(ext) <= 3
        and not filename.startswith('.')
        and not _has_invalid_dos_character(filename)
    )


def _is_valid_vfat_filename(filename: str) -> bool:
    """Return whether ``filename`` is a valid VFAT filename."""
    try:
        filename.encode('utf-16le')
    except UnicodeEncodeError:
        return False

    return (
        0 < len(filename) <= VFAT_FILENAME_MAX_LENGTH
        and not filename.endswith(' ')
        and not filename.endswith('.')
        and not any(char in VFAT_FILENAME_FORBIDDEN for char in filename)
    )


def _check_vfat_filename(filename: str) -> None:
    """Check that ``filename`` is a valid VFAT filename.

    Because VFAT filenames are a superset of DOS filenames, this is a check every new
    filename must pass.
    """
    if not _is_valid_vfat_filename(filename):
        raise ValidationError(f'Invalid filename {filename!r}')


def _requires_vfat(filename: str) -> bool:
    """Return whether storing ``filename`` requires the VFAT extension to be enabled.

    This function is separate from ``_to_be_saved_as_vfat()`` because setting case
    info for a filename – which in theory would not require any VFAT LFN entries – is
    only supported when VFAT is enabled.
    """
    return not _is_valid_dos_filename(filename)


def _to_be_saved_as_vfat(filename: str) -> bool:
    """Return whether a VFAT LFN must be used to correctly store ``filename``.

    If not, a single 8.3 entry with case info can be used.
    """
    name, ext = _split_filename(filename)
    return (
        not (name == '' or name.isupper() or name.islower())
        or not (ext == '' or ext.isupper() or ext.islower())
        # Pass filename.upper() because _is_valid_dos_filename() rejects lowercase parts
        or not _is_valid_dos_filename(filename.upper())
    )


def _get_case_info(filename: str) -> int:
    """Return case information flags for ``filename``.

    Assumption: ``filename.upper()`` is a valid DOS filename.
    """
    name, ext = _split_filename(filename)
    case_info = 0

    if name.islower():
        case_info |= CASE_INFO_NAME_LOWER
    if ext.islower():
        case_info |= CASE_INFO_EXT_LOWER

    return case_info


def _vfat_filename_checksum(filename: str) -> int:
    """Return the checksum of VFAT filename ``filename`` used in case of a DOS
    filename collision in a directory.

    This function tries to mimic Windows NT behavior as closely as possible and uses
    ``ctypes`` to force the wrap-around of integers.

    Source: https://tomgalvin.uk/blog/gen/2015/06/09/filenames/.
    """
    checksum = c_uint16(0)
    for char in filename:
        try:
            # Using the OEM encoding seems to be exactly what Windows does here. The
            # only thing I wasn't able to find out is how their checksum function
            # deals with characters not available in the OEM encoding, e.g. '☺'. So
            # if such characters are used in LFNs, the checksum generated here will
            # not match the one Windows would generate.
            char_int = char.encode(DOS_FILENAME_OEM_ENCODING)[0]
        except UnicodeEncodeError:
            char_int = 0xFE  # dummy

        checksum = c_uint16((((checksum.value * 0x25) & 0xFFFF) + char_int) & 0xFFFF)

    pi_thing = c_int32(checksum.value * 314159269)
    pi_thing.value = abs(pi_thing.value)

    shifted = c_uint64((c_int64(pi_thing.value).value * 1152921497) >> 60)
    seven_thing = c_int32(pi_thing.value - shifted.value * 1000000007)
    checksum = c_uint16(seven_thing.value)

    reversed_ = c_uint16(
        ((checksum.value & 0xF000) >> 12)
        | ((checksum.value & 0x0F00) >> 4)
        | ((checksum.value & 0x00F0) << 4)
        | ((checksum.value & 0x000F) << 12)
    )
    return reversed_.value


def _vfat_to_dos_filename(filename: str, existing_filenames: Iterable[str]) -> str:
    """Return DOS filename for VFAT filename ``filename``.

    :param existing_filenames: DOS filenames present in the target directory.

    Assumption: ``filename`` is not already present in the target directory.
    """
    filename_upper = filename.upper()
    name, ext = _split_filename(filename_upper)

    def sanitize(part_: str) -> str:
        """Remove all dots and spaces of ``part_`` and replace each character not
        allowed in DOS filenames with an underscore.
        """
        sanitized_ = part_.replace('.', '').replace(' ', '')
        for index, char in enumerate(sanitized_):
            if _is_invalid_dos_character(char):
                sanitized_ = sanitized_[:index] + '_' + sanitized_[index + 1 :]
        return sanitized_

    name_sanitized = sanitize(name)
    ext_sanitized = sanitize(ext)

    sanitized = f'{name_sanitized[:8]}.{ext_sanitized[:3]}'.rstrip('.')
    sanitizing_did_something = sanitized != filename_upper

    if _is_valid_dos_filename(filename_upper) and not sanitizing_did_something:
        return filename_upper

    # A tilde (~) variant of the filename is now definitely required.

    if filename.startswith('.') and name.lstrip('.').lstrip(' ') == '':
        # In certain special cases, ext is used as name.
        name_6 = ext_sanitized[:6]
        ext_3 = ''
    else:
        name_6 = name_sanitized[:6]
        ext_3 = ext_sanitized[:3]

    existing_filenames_split = (_split_filename(fn) for fn in existing_filenames)
    existing_names_ext_match = [
        fn[0] for fn in existing_filenames_split if fn[1] == ext_3
    ]

    # For sanitized names not longer than 2 characters, the two-letter tilde variant
    # is used.
    if len(name_6) > 2:
        for i in range(1, 5):
            proposed_name = f'{name_6}~{i}'
            if proposed_name not in existing_names_ext_match:
                found = f'{proposed_name}.{ext_3}'
                return found.rstrip('.')

    # A two-(or-less-)letter variant of the filename is now definitely required.
    checksum = _vfat_filename_checksum(filename)
    new_name_6 = f'{name_6[:2]}{checksum:04X}'

    for char_count in range(len(new_name_6), -1, -1):
        new_name_part = new_name_6[:char_count]
        exp = len(new_name_6) - char_count  # (0, 1, 2, ...)

        for i in range(10**exp, 10 ** (exp + 1)):
            proposed_name = f'{new_name_part}~{i}'
            if proposed_name not in existing_names_ext_match:
                found = f'{proposed_name}.{ext_3}'
                return found.rstrip('.')

    raise FileSystemLimit(
        f'Could not find a DOS filename for VFAT filename {filename}'
    )  # pragma: nocover


def _dos_filename_checksum(name_bytes: bytes, ext_bytes: bytes) -> int:
    """Return checksum of a packed DOS filename; used for VFAT entries (offset 0x0D)."""
    checksum = c_uint8(0)
    for byte in name_bytes + ext_bytes:
        checksum = c_uint8(((checksum.value & 1) << 7) + (checksum.value >> 1) + byte)
    return checksum.value


def pack_dos_datetime(dt: datetime) -> tuple[int, int, int]:
    """Return a packed DOS datetime as a tuple of (date, time, 10 ms count) from the
    datetime object ``dt``.
    """
    if dt.year < DOS_YEAR_MIN or dt.year > DOS_YEAR_MAX:
        raise ValueError(f'Invalid DOS date {dt}')
    date = ((dt.year - DOS_YEAR_MIN) << 9) | (dt.month << 5) | dt.day
    time = (dt.hour << 11) | (dt.minute << 5) | (dt.second // 2)
    time_ten_ms = (dt.second % 2) * 100 + dt.microsecond // 10_000
    return date, time, time_ten_ms


def unpack_dos_datetime(
    date: int, time: int = 0, time_ten_ms: int = 0
) -> datetime | None:
    """Return a datetime object from a DOS datetime packed as ``date``, ``time`` and
    ``time_ten_ms`` (10 ms count) values or ``None`` if the values passed do not
    represent an valid DOS datetime.
    """
    # Restriction because of the two-second resolution of the seconds field
    if time_ten_ms >= DOS_TIME_TEN_MS_MAX:
        return None

    y = ((date & 0b1111111000000000) >> 9) + DOS_YEAR_MIN
    m = (date & 0b0000000111100000) >> 5
    d = date & 0b0000000000011111
    hh = (time & 0b1111100000000000) >> 11
    mm = (time & 0b0000011111100000) >> 5
    ss = (time & 0b0000000000011111) * 2 + time_ten_ms // 100
    us = (time_ten_ms % 100) * 10_000
    try:
        return datetime(y, m, d, hh, mm, ss, us)
    except ValueError:
        return None


@dataclass(frozen=True)
class EightDotThreeEntry(ByteStruct):
    """8.3 directory entry."""

    name: Annotated[bytes, 8]
    extension: Annotated[bytes, 3]
    _attributes: Annotated[int, 1]
    case_info_vfat: Annotated[int, 1]
    created_time_ten_ms: Annotated[int, 1]
    created_time: Annotated[int, 2]
    created_date: Annotated[int, 2]
    last_accessed_date: Annotated[int, 2]
    _cluster_high_fat_32: Annotated[int, 2]
    last_modified_time: Annotated[int, 2]
    last_modified_date: Annotated[int, 2]
    _cluster: Annotated[int, 2]
    size: Annotated[int, 4]

    def filename(self, *, vfat: bool) -> str:
        """Filename; case information is applied if VFAT support is enabled."""
        unpacked = _unpack_dos_filename(self.name, self.extension)
        if not vfat:
            return unpacked.rstrip('.')

        name, ext = _split_filename(unpacked)
        if self.case_info_vfat & CASE_INFO_NAME_LOWER:
            name = name.lower()
        if self.case_info_vfat & CASE_INFO_EXT_LOWER:
            ext = ext.lower()

        return f'{name}.{ext}'.rstrip('.')

    @property
    def dos_filename(self) -> str:
        """DOS filename."""
        unpacked = _unpack_dos_filename(self.name, self.extension)
        return unpacked.rstrip('.')

    def cluster(self, *, fat_32: bool) -> int:
        """Start cluster of the file or directory."""
        if not fat_32:
            return self._cluster
        return (self._cluster_high_fat_32 << 16) | self._cluster

    @property
    def hint(self) -> Hint | None:
        """Special meaning of the directory entry or ``None``."""
        try:
            return Hint(self.name[0])
        except ValueError:
            return None

    @property
    def attributes(self) -> Attributes:
        """Directory entry attributes."""
        return Attributes(self._attributes)

    @property
    def volume_label(self) -> bool:
        """Whether the directory entry represents a volume label."""
        return (
            Attributes.VOLUME_LABEL in self.attributes
            and Attributes.VFAT not in self.attributes
        )

    @property
    def created(self) -> datetime | None:
        """Creation datetime or ``None`` if invalid."""
        return unpack_dos_datetime(
            self.created_date, self.created_time, self.created_time_ten_ms
        )

    @property
    def last_accessed(self) -> datetime | None:
        """Datetime of last access or ``None`` if invalid."""
        return unpack_dos_datetime(self.last_accessed_date)

    @property
    def last_modified(self) -> datetime | None:
        """Datetime of last modification or ``None`` if invalid."""
        return unpack_dos_datetime(self.last_modified_date, self.last_modified_time)


@dataclass(frozen=True)
class VfatEntry(ByteStruct):
    """VFAT long filename entry."""

    seq: Annotated[int, 1]
    chars_1: Annotated[bytes, 10]
    attributes: Annotated[int, 1]
    type: Annotated[int, 1]
    checksum: Annotated[int, 1]  # checksum of DOS file name
    chars_2: Annotated[bytes, 12]
    cluster: Annotated[int, 2]
    chars_3: Annotated[bytes, 4]

    def validate(self) -> None:
        if Attributes.VFAT not in Attributes(self.attributes):
            raise ValidationError(
                f'Invalid attributes {self.attributes} for VFAT entry'
            )
        if not 1 <= self.number <= MAX_VFAT_ENTRIES:
            raise ValidationError(
                f'Sequence number must be in range (1, {MAX_VFAT_ENTRIES})'
            )
        if self.cluster != 0:
            raise ValidationError('Cluster number in VFAT entry must be 0')

    @property
    def first_lfn_entry(self) -> bool:
        """Whether this is the first physical VFAT entry of a VFAT entry chain."""
        return bool(self.seq & VFAT_FIRST_LFN_ENTRY)

    @property
    def number(self) -> int:
        """Sequence number of the VFAT entry."""
        return self.seq & VFAT_ENTRY_NUMBER_MASK


class Entry:
    """Directory entry in a FAT file system.

    Always holds an 8.3 entry. If VFAT support is enabled, it may also hold up to 20
    VFAT entries.

    VFAT entries are stored in physical order, i.e. as on the disk.
    """

    def __init__(
        self,
        eight_dot_three: EightDotThreeEntry,
        vfat_entries: Iterable[VfatEntry] = (),
        *,
        vfat: bool,
        fat_32: bool,
    ):
        vfat_entries = tuple(vfat_entries)

        if vfat_entries and not vfat:
            raise ValueError('VFAT entries passed but VFAT support is disabled')
        if eight_dot_three.hint is not None:
            raise ValueError(
                f'8.3 entry must not be a special 8.3 entry with {eight_dot_three.hint}'
            )
        if eight_dot_three.volume_label:
            raise ValueError('8.3 entry must not be a volume label entry')
        if Attributes.VFAT in eight_dot_three.attributes:
            raise ValueError('8.3 entry must not be a VFAT entry')

        if len(vfat_entries) > MAX_VFAT_ENTRIES:
            raise ValidationError(
                f'VFAT entry chain must not contain more than {MAX_VFAT_ENTRIES} '
                f'entries'
            )

        if vfat_entries:
            if not vfat_entries[0].first_lfn_entry:
                raise ValidationError(
                    'First VFAT entry does not have bit 6 of sequence number set'
                )

            # Check DOS filename checksum
            expected_checksum = _dos_filename_checksum(
                eight_dot_three.name, eight_dot_three.extension
            )
            for vfat_entry in vfat_entries:
                if vfat_entry.checksum != expected_checksum:
                    # This means that the DOS file name was changed but the VFAT file
                    # name was not.
                    raise ValidationError(
                        'Checksum in VFAT entry does not match checksum of DOS filename'
                    )

        self._eight_dot_three = eight_dot_three
        self._vfat_entries = vfat_entries
        self._vfat = vfat
        self._fat_32 = fat_32

    def __bytes__(self) -> bytes:
        """``bytes`` form of all directory entries represented by this generalized
        entry in physical order.
        """
        # noinspection PyTypeChecker
        return b''.join(map(bytes, self._vfat_entries)) + bytes(self._eight_dot_three)

    @property
    def filename(self) -> str:
        """Filename; VFAT long filename if VFAT support is enabled."""
        # TODO: Move volume label parsing to FileSystem
        # if self.volume_label:
        #     return self._eight_dot_three.filename(vfat=False).replace('.', '')

        if self._vfat_entries:
            filename_bytes = b''
            for entry in reversed(self._vfat_entries):
                filename_bytes += entry.chars_1 + entry.chars_2 + entry.chars_3
            filename = filename_bytes.decode('utf-16le')
            return filename.rstrip('\x00\uffff').rstrip('. ')

        return self._eight_dot_three.filename(vfat=self._vfat)

    @property
    def dos_filename(self) -> str:
        """DOS filename."""
        return self._eight_dot_three.dos_filename

    @property
    def cluster(self) -> int:
        """Start cluster of the file or directory."""
        return self._eight_dot_three.cluster(fat_32=self._fat_32)

    @property
    def attributes(self) -> Attributes:
        """Directory entry attributes."""
        return self._eight_dot_three.attributes

    @property
    def created(self) -> datetime | None:
        """Creation datetime or ``None`` if invalid."""
        return self._eight_dot_three.created

    @property
    def last_accessed(self) -> datetime | None:
        """Datetime of last access or ``None`` if invalid."""
        return self._eight_dot_three.last_accessed

    @property
    def last_modified(self) -> datetime | None:
        """Datetime of last modification or ``None`` if invalid."""
        return self._eight_dot_three.last_modified

    @property
    def size(self) -> int:
        """File size in bytes; zero for directories and empty files."""
        return self._eight_dot_three.size

    @property
    def total_entries(self) -> int:
        """Total number of directory entries represented by this generalized entry."""
        return 1 + len(self._vfat_entries)

    @property
    def eight_dot_three(self) -> EightDotThreeEntry:
        """8.3 entry."""
        return self._eight_dot_three

    @property
    def vfat_entries(self) -> tuple[VfatEntry, ...]:
        """VFAT entries in physical order; i.e. as on the disk."""
        return self._vfat_entries

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Entry):
            return (
                self._eight_dot_three == other._eight_dot_three
                and self._vfat_entries == other._vfat_entries
                # We compare the following attributes because they affect the
                # properties of the entry.
                and self._vfat == other._vfat
                and self._fat_32 == other._fat_32
            )
        return NotImplemented

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}({self.filename!r}, dos_filename='
            f'{self.dos_filename!r}, attributes={self.attributes}, cluster='
            f'{self.cluster}, size={self.size}, total_entries={self.total_entries})'
        )


def entry_match(part: str, entry: Entry, *, vfat: bool) -> bool:
    """Check whether ``part`` matches the file name stored in ``entry``.

    If VFAT support is enabled, i.e. ``vfat`` is ``True``, ``part`` is additionally
    checked against the DOS file name stored in ``entry``.
    """
    return part.upper() == entry.filename.upper() or (
        vfat and part.upper() == entry.dos_filename
    )


@overload
def iter_entries(
    bytes_iter: Iterable[bytes],
    *,
    only_useful: Literal[True] = ...,
    vfat: bool,
    fat_32: bool,
) -> Iterator[Entry]:
    ...


@overload
def iter_entries(
    bytes_iter: Iterable[bytes],
    *,
    only_useful: Literal[False] = ...,
    vfat: bool,
    fat_32: bool,
) -> Iterator[Entry | EightDotThreeEntry]:
    ...


def iter_entries(
    bytes_iter: Iterable[bytes],
    *,
    only_useful: Literal[False, True] = True,
    vfat: bool,
    fat_32: bool,
) -> Iterator[Entry | EightDotThreeEntry]:
    """Yield directory entries found in ``bytes_iter``.

    Each element of ``bytes_iter`` should represent the ``bytes`` form of one 8.3
    entry, i.e. 32 bytes.

    Directory entries are yielded either as ``Entry`` -- if an entry or VFAT entry
    chain is considered useful -- or as ``EightDotThreeEntry`` -- if an entry is not
    considered useful.
    Entries starting with ``0x00`` mark the end of the directory table and are not
    yielded.

    A directory entry is *not* considered useful if it is:

    - A deleted entry (starting with ``0xE5``).
    - A dot entry (starting with ``0x2E``).
    - A VFAT entry and VFAT support is disabled.
    - A non-VFAT entry which has the volume label attribute set.
    - Part of a VFAT entry chain which does not match its assigned 8.3 entry or
        violates any other rule checked against for VFAT entry chains. In this case
        it is assumed that the according 8.3 entry was edited by a system which does
        not support VFAT: All VFAT entries in the chain are considered useless and
        only the 8.3 entry succeeding the chain is yielded as ``Entry``.

    If ``only_useful`` is set to ``True``, only entries considered useful, meaning
    instances of ``Entry``, are yielded.
    """
    cursor = 0  # current byte

    # collected entries in VFAT chain
    pending_edt_entries: list[EightDotThreeEntry] = []
    pending_vfat_entries: list[VfatEntry] = []

    def clear_pending() -> None:
        pending_edt_entries.clear()
        pending_vfat_entries.clear()

    for entry_bytes in bytes_iter:
        edt_entry = EightDotThreeEntry.from_bytes(entry_bytes)

        if edt_entry.hint is Hint.END_OF_ENTRIES:
            yield from pending_edt_entries
            break

        if (
            edt_entry.hint is Hint.DELETED
            or edt_entry.hint is Hint.DOT_ENTRY
            or edt_entry.volume_label
        ):
            # keep, but don't really deal with them
            yield from pending_edt_entries
            if not only_useful:
                yield edt_entry
            clear_pending()

        elif Attributes.VFAT in edt_entry.attributes:
            if vfat:
                if not only_useful:
                    pending_edt_entries.append(edt_entry)
                try:
                    vfat_entry = VfatEntry.from_bytes(entry_bytes)
                except ValidationError:
                    log.warning(f'Failed to parse VFAT entry {edt_entry}')
                    yield from pending_edt_entries
                    clear_pending()
                else:
                    pending_vfat_entries.append(vfat_entry)
            elif not only_useful:
                yield edt_entry  # fallback if no VFAT support

        else:
            # useful entry
            try:
                yield Entry(edt_entry, pending_vfat_entries, vfat=vfat, fat_32=fat_32)
            except ValidationError:
                # continue with 8.3 entry only
                log.warning(
                    f'Discarded VFAT entries for 8.3 entry {edt_entry.dos_filename!r}'
                )
                yield from pending_edt_entries
                yield Entry(edt_entry, (), vfat=vfat, fat_32=fat_32)

            clear_pending()

        cursor += ENTRY_SIZE


def create_entry(
    existing_entries: Collection[Entry],
    filename: str,
    attributes: Attributes,
    created: datetime,
    last_accessed: datetime,
    last_modified: datetime,
    cluster: int = 0,
    size: int = 0,
    *,
    vfat: bool,
    fat_32: bool,
) -> Entry:
    """Create new entry for directory with ``existing_entries``.

    Not to be used to create a volume label.
    """
    if Attributes.VOLUME_LABEL in attributes:
        raise ValueError('New entry must not have the volume label attribute')

    filename = filename.rstrip('. ')
    _check_vfat_filename(filename)
    requires_vfat = _requires_vfat(filename)
    to_be_saved_as_vfat = _to_be_saved_as_vfat(filename)

    case_info = 0
    vfat_entries = []  # physical order

    if requires_vfat and not vfat:
        raise ValueError(f'File name {filename!r} requires VFAT')

    # check whether a file with the same file name already exists
    for entry in existing_entries:
        if entry_match(filename, entry, vfat=vfat):
            raise ValueError(f'File with name {filename!r} already exists')

    if requires_vfat and to_be_saved_as_vfat:
        # 8.3 entry and VFAT entry chain
        already_existing_dos = (entry.dos_filename for entry in existing_entries)
        dos_filename = _vfat_to_dos_filename(filename, already_existing_dos)

        # 13 chars per VFAT entry, physical order
        for physical_index, start in enumerate(reversed(range(0, len(filename), 13))):
            # physical_index: 0, 1, 2, ...
            # start: ..., 26, 13, 0
            chars_str = filename[start : start + 13]
            chars = chars_str.encode('utf-16le')
            seq = start // 13 + 1  # ..., 3, 2, 1 (logical)

            if physical_index == 0:
                if len(chars_str) < 13:
                    chars += b'\x00\x00'
                    chars = chars.ljust(26, b'\xFF')
                seq |= VFAT_FIRST_LFN_ENTRY

            chars_1, chars_2, chars_3 = chars[:10], chars[10:22], chars[22:]
            checksum = _dos_filename_checksum(*_pack_dos_filename(dos_filename))
            vfat_entry = VfatEntry(
                seq, chars_1, Attributes.VFAT.value, 0, checksum, chars_2, 0, chars_3
            )
            vfat_entries.append(vfat_entry)

    elif requires_vfat and not to_be_saved_as_vfat:
        # only use standard 8.3 entry and set case information
        dos_filename = filename.upper()
        case_info = _get_case_info(filename)

    else:
        # does not require VFAT, only use standard 8.3 entry
        dos_filename = filename

    # pack data
    packed_name, packed_ext = _pack_dos_filename(dos_filename)
    created_date, created_time, created_time_ten_ms = pack_dos_datetime(created)
    last_accessed_date, _, _ = pack_dos_datetime(last_accessed)
    last_modified_date, last_modified_time, _ = pack_dos_datetime(last_modified)

    cluster_low = cluster & 0xFF
    cluster_high = cluster >> 16

    if not fat_32 and cluster_high != 0:
        raise ValueError(
            'High bits of cluster number can only be used on FAT32 file systems'
        )

    edt_entry = EightDotThreeEntry(
        packed_name,
        packed_ext,
        attributes.value,
        case_info,
        created_time_ten_ms,
        created_time,
        created_date,
        last_accessed_date,
        cluster_high,
        last_modified_time,
        last_modified_date,
        cluster_low,
        size,
    )
    return Entry(edt_entry, vfat_entries, vfat=vfat, fat_32=fat_32)


def updated_entry(
    entry: Entry,
    new_cluster: int = None,
    new_size: int = None,
    last_accessed: datetime = None,
    last_modified: datetime = None,
    *,
    vfat: bool,
    fat_32: bool,
) -> Entry:
    if all(
        value is None for value in (new_cluster, new_size, last_accessed, last_modified)
    ):
        return entry

    old_edt_entry = entry.eight_dot_three
    replacements: dict[str, Any] = {}

    if new_cluster is not None:
        cluster_low = new_cluster & 0xFF
        cluster_high = new_cluster >> 16
        if not fat_32 and cluster_high != 0:
            raise ValueError(
                'High bits of cluster number can only be used on FAT32 file systems'
            )
        replacements['_cluster'] = cluster_low
        replacements['_cluster_high_fat_32'] = cluster_high

    if new_size is not None:
        replacements['size'] = new_size

    if last_accessed is not None:
        last_accessed_date, _, _ = pack_dos_datetime(last_accessed)
        replacements['last_accessed_date'] = last_accessed_date

    if last_modified is not None:
        last_modified_date, last_modified_time, _ = pack_dos_datetime(last_modified)
        replacements['last_modified_date'] = last_modified_date
        replacements['last_modified_time'] = last_modified_time

    new_edt_entry = replace(old_edt_entry, **replacements)
    new_entry = Entry(new_edt_entry, entry.vfat_entries, vfat=vfat, fat_32=fat_32)
    return new_entry
