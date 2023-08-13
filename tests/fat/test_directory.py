"""Tests for the ``directory`` module of the ``fat`` package."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

import pytest

from diskfs.base import ValidationError

# noinspection PyProtectedMember
from diskfs.fat.directory import (
    CASE_INFO_EXT_LOWER,
    CASE_INFO_NAME_LOWER,
    DOS_FILENAME_OEM_ENCODING,
    Attributes,
    EightDotThreeEntry,
    Hint,
    VfatEntry,
    _check_vfat_filename,
    _dos_filename_checksum,
    _get_case_info,
    _is_valid_dos_filename,
    _is_valid_vfat_filename,
    _pack_dos_filename,
    _requires_vfat,
    _split_filename,
    _to_be_saved_as_vfat,
    _unpack_dos_filename,
    _vfat_filename_checksum,
    _vfat_to_dos_filename,
    pack_dos_datetime,
    unpack_dos_datetime,
)

E5 = b'\xE5'.decode(DOS_FILENAME_OEM_ENCODING)


@pytest.mark.parametrize(
    ['filename', 'name', 'ext'],
    [
        ('thing.json', 'thing', 'json'),
        ('thing.json.txt', 'thing.json', 'txt'),
        ('thing', 'thing', ''),
        ('.thing', '', 'thing'),
        ('thing.', 'thing', ''),
        ('. thing', '', ' thing'),
        ('.  thing', '', '  thing'),
        ('.thing.', '.thing', ''),
        ('.thing  .', '.thing  ', ''),
        ('thing.json.', 'thing.json', ''),
        ('Thing.jsoN', 'Thing', 'jsoN'),
        ('thinG.Json', 'thinG', 'Json'),
        ('thin9.Json', 'thin9', 'Json'),
    ],
)
def test__split_filename(filename, name, ext):
    """Test splitting of ``_split_filename()``."""
    assert _split_filename(filename) == (name, ext)


@pytest.mark.parametrize(
    ['filename', 'name_packed', 'ext_packed'],
    [
        ('COFFEE', b'COFFEE  ', b'   '),
        ('COFFEE.C', b'COFFEE  ', b'C  '),
        ('COFFEE.CA', b'COFFEE  ', b'CA '),
        ('C0FFEE.CA', b'C0FFEE  ', b'CA '),
        ('COFFEE.CAF', b'COFFEE  ', b'CAF'),
        ('CAFFEINE.CAF', b'CAFFEINE', b'CAF'),
        ('CAFFEINE.CA', b'CAFFEINE', b'CA '),
        ('CAFFEINE', b'CAFFEINE', b'   '),
        (f'{E5}OFFEE.C', b'\x05OFFEE  ', b'C  '),  # Note: \x05 is forbidden anyway
        (f'C{E5}FFEE.C', b'C\xE5FFEE  ', b'C  '),
        (f'COFFEE.C{E5}', b'COFFEE  ', b'C\xE5 '),
    ],
)
def test__pack_unpack_dos_filename(filename, name_packed, ext_packed):
    """Test DOS filename packing and unpacking via ``_pack_dos_filename()`` and
    ``_unpack_dos_filename()``.
    """
    assert _pack_dos_filename(filename) == (name_packed, ext_packed)
    assert _unpack_dos_filename(name_packed, ext_packed) == filename


@pytest.mark.parametrize(
    'filename',
    [
        'COFFEE',
        'CO FEE',
        'COFFEE.EXT',
        'COFFEE.E T',
        'COFFEE.CAF',
        'COFFEE.CA',
        'COFFEE.C',
        'CAFFEINE.CAF',
        'CAFFEINE.CA',
        'CAFFEINE.C',
        'C4FFEINE.C',
        'C4FFEINE.C4F',
        '123.456',
        '1.2',
        'A.A',
        'A',
        'THYCUP',
        'THY CUP',
        'THYCÜP',
        ' .  A',
        ' . A',
        ' .ABC',
        '        .  A',
        '       A.  A',
        '        .  Ä',
        'CÖFFEE',
        'CÖFFEE.ÄXT',
        'COFFEE!.EXT',
        '!COFFEE!.EX!',
        '1#COFFEE.PL$',
        '01234567.890',
        '(1234)6.{((',
        'A B',
        'A B.C D',
        *(f'{c}.AB' for c in ' !#$%&\'()-@^_`{}~'),
        *(f'A{c}.B' for c in ' !#$%&\'()-@^_`{}~'),
        *(f'AB.{c}' for c in '!#$%&\'()-@^_`{}~'),
    ],
)
def test_valid_dos_filename(filename):
    """Test that ``filename`` is considered a valid DOS filename."""
    assert _is_valid_dos_filename(filename)
    assert _is_valid_vfat_filename(filename)
    assert not _requires_vfat(filename)


@pytest.mark.parametrize(
    ['filename', 'saved_as_vfat'],
    [
        ('爱', True),
        ('★', True),
        ('coffee', False),
        ('cöffee', False),
        ('cöffee.äxt', False),
        ('cöffee.ÄXT', False),
        ('CÖFFEE.äxt', False),
        ('CöFFEE', True),
        ('coFFee', True),
        ('CoFFeE', True),
        ('CoFFeE.ExT', True),
        ('cOffee.eXt', True),
        ('COFFEE.ext', False),
        ('coffee.EXT', False),
        ('CAFFEINEE.EXT', True),
        ('CAFFEINATED.EXT', True),
        ('COFFEE.EXXT', True),
        ('a' * 255, True),
        ('A' * 255, True),
        ('.A' * 127, True),
        ('a', False),
        ('.A', True),
        ('A..B', True),
        ('.A.B', True),
        ('..B', True),
        ('.AB', True),
        ('.ABC', True),
        ('. .A', True),
        ('. .a', True),
        ('A b', True),
        ('a b', False),
        ('a b.c d', False),
        ('. A', True),
        ('   a', False),
        ('   a   B', True),
        ('   A.   b', True),
        ('   A   .b', False),
        ('   A     .b', True),
        (' .  .. A', True),
        (' .   A', True),
        ('.  Ä', True),
        *((f'{c}.AB', True) for c in '+,;=[]'),
        *((f'A{c}.B', True) for c in '+,;=[]'),
        *((f'AB.{c}', True) for c in '+,;=[]'),
    ],
)
def test_valid_vfat_but_invalid_dos_filename(filename, saved_as_vfat):
    """Test that ``filename`` is considered a valid VFAT filename but not a valid DOS
    filename and whether a VFAT LFN is required to store the filename.
    """
    assert _is_valid_vfat_filename(filename)
    _check_vfat_filename(filename)
    assert not _is_valid_dos_filename(filename)
    assert _requires_vfat(filename)
    assert _to_be_saved_as_vfat(filename) is saved_as_vfat


@pytest.mark.parametrize(
    'filename',
    [
        'a' * 256,
        'a.' * 127,
        'a.',
        'ab.',
        'ab.ext.',
        'a ',
        'a b ',
        '.',
        '..',
        '...',
        ' ',
        '  ',
        '  ',
        ' .',
        '. ',
        '',
        *(f'{c}.ab' for c in '\x00\x05\x1F"*/:<>?\\|\x7F'),
        *(f'a{c}.b' for c in '\x00\x05\x1F"*/:<>?\\|\x7F'),
        *(f'ab.{c}' for c in '\x00\x05\x1F"*/:<>?\\|\x7F'),
        *'"*/:<>?\\|\x7F',
        '\uD800',
        'are\uDEADserious',
    ],
)
def test_invalid_vfat_filename(filename):
    """Test that ``filename`` is not considered a valid VFAT filename."""
    assert not _is_valid_vfat_filename(filename)
    assert not _is_valid_dos_filename(filename)
    with pytest.raises(ValidationError, match='Invalid filename.*'):
        _check_vfat_filename(filename)


@pytest.mark.parametrize(
    ['filename', 'name_lower', 'ext_lower'],
    [
        ('coffee', True, False),
        ('cöffee', True, False),
        ('coffee.ext', True, True),
        ('cöffee.äxt', True, True),
        ('coffee.EXT', True, False),
        ('cöffee.ÄXT', True, False),
        ('COFFEE.ext', False, True),
        ('CÖFFEE.äxt', False, True),
        ('COFFEE.EXT', False, False),
        ('CÖFFEE.ÄXT', False, False),
    ],
)
def test__get_case_info(filename, name_lower, ext_lower):
    """Test that ``_get_case_info()`` returns the correct case information flags for
    ``filename``.
    """
    assert _is_valid_dos_filename(filename.upper())  # internal check
    assert bool(_get_case_info(filename) & CASE_INFO_NAME_LOWER) is name_lower
    assert bool(_get_case_info(filename) & CASE_INFO_EXT_LOWER) is ext_lower


@pytest.mark.parametrize(
    ['filename', 'checksum'],
    [
        ('filename_01', 0x6580),
        ('filename_02', 0xBF8B),
        ('filename_03', 0xB483),
        ('filename_04', 0xDA15),
        ('filename_05', 0x801A),
        ('filename_06', 0x360F),
        ('filename_07', 0x5C90),
        ('filename_08', 0x0295),
        ('FiLeNaMe_09', 0xDE8C),
        ('FILENAME_10', 0xA80B),
        ('caffeinated.caf.caf', 0xF883),
        ('caffeinated.cäf.caf', 0xB976),
        ('caffeinated.caffeinated', 0x7E22),
        ('caffeinated_coffee.caf', 0x0153),
        ('caffeinated_+,;=[].caf', 0x0CFA),
        ('caffeinated_({}).caf', 0xAF27),
        ('caffeinated_☺.caf', _vfat_filename_checksum('caffeinated_☻.caf')),  # dummy
    ],
)
def test__vfat_filename_checksum(filename, checksum):
    """Test that ``_vfat_filename_checksum()`` returns the expected checksum for
    ``filename``.
    """
    assert _vfat_filename_checksum(filename) == checksum


@pytest.mark.parametrize(
    ['filename', 'dos_filename'],
    [
        ('COFFEE.CAF', 'COFFEE.CAF'),
        ('COFFEE.caf', 'COFFEE.CAF'),
        ('coffee.CAF', 'COFFEE.CAF'),
        ('coffee.caf', 'COFFEE.CAF'),
        ('cof.fee.caf', 'COFFEE~1.CAF'),
        ('.coffee.caf', 'COFFEE~1.CAF'),
        ('.cof.fee.caf', 'COFFEE~1.CAF'),
        (' . c.  o .ffee.caf', 'COFFEE~1.CAF'),
        ('coffee.cafe', 'COFFEE~1.CAF'),
        ('coffee.c a f e', 'COFFEE~1.CAF'),
        ('coFFee.C a f e', 'COFFEE~1.CAF'),
        ('caffeinated.C a f e', 'CAFFEI~1.CAF'),
        ('caffeinated.caffeinated', 'CAFFEI~1.CAF'),
        ('C☺FFEE.C☻F', 'C_FFEE~1.C_F'),
        ('☺☻☺☻☺☻.☻☺☻', '______~1.___'),
        ('☺☻☺☻☺☻☺☻☺☻☺☻.☻☺☻☺☻', '______~1.___'),
        ('☺☻☺☻☺☻☺☻☺☻☺☻.A☻☺', '______~1.A__'),
        ('______~1.___', '______~1.___'),
        (',+,', '___~1'),
        ('coffeecoffee', 'COFFEE~1'),
        ('.coffeecoffee', 'COFFEE~1'),
        ('.coffee', 'COFFEE~1'),
        ('. coffee', 'COFFEE~1'),
        ('AB', 'AB'),
        ('aB', 'AB'),
        ('A', 'A'),
        ('b', 'B'),
        ('_', '_'),
        ('1-', '1-'),
        ('A+', 'A_0D92~1'),
        (',b', '_BF6E2~1'),
        (',+', '__3050~1'),
        (',', '_C5C5~1'),
        ('+', '_7BBA~1'),
        (' A', 'AEFA0~1'),
        ('.A', 'ACD87~1'),
        ('.  [', '_D34E~1'),
        ('.[...A+', '_D9F5~1.A_'),
        ('A.[', 'AB33A~1._'),
        ('ABC.[', 'ABC~1._'),
        ('COFFEE', 'COFFEE'),
        (' COFFEE', 'COFFEE~1'),
        ('.COFFEE', 'COFFEE~1'),
        ('  COFFEE', 'COFFEE~1'),
        (' .COFFEE', 'AE89~1.COF'),
        ('. COFFEE', 'COFFEE~1'),
        ('..COFFEE', 'COFFEE~1'),
        ('   COFFEE', 'COFFEE~1'),
        ('  .COFFEE', 'FADF~1.COF'),
        (' . COFFEE', 'BA0E~1.COF'),
        (' ..COFFEE', '40B4~1.COF'),
        ('.  COFFEE', 'COFFEE~1'),
        ('. .COFFEE', 'COFFEE~1'),
        ('.. COFFEE', 'COFFEE~1'),
        ('...COFFEE', 'COFFEE~1'),
        ('[[[COFFEE', '___COF~1'),
        ('.[.COFFEE', '_051A~1.COF'),
        ('[.[COFFEE', '_C052~1._CO'),
        ('#.!COFFEE', '#F0E3~1.!CO'),
        ('. . CO', 'COB5A1~1'),
        ('123[.456', '123_~1.456'),
    ],
)
def test__vfat_to_dos_filename_single(filename, dos_filename):
    """Test DOS filename generation from VFAT filenames for empty directories."""
    assert _vfat_to_dos_filename(filename, []) == dos_filename


@pytest.mark.parametrize(
    'pairs',
    [
        [
            ('caffeine_01', 'CAFFEI~1'),
            ('caffeine_02', 'CAFFEI~2'),
            ('caffeine_03', 'CAFFEI~3'),
            ('caffeine_04', 'CAFFEI~4'),
            ('caffeine_05', 'CA517E~1'),
            ('caffeine_06', 'CA7700~1'),
            ('caffeine_07', 'CA2DF4~1'),
            ('caffeine_08', 'CAD2F9~1'),
            ('caffeine_09', 'CAF88B~1'),
            ('caffeine_10', 'CA3866~1'),
            ('caffeine_11', 'CAED5B~1'),
            ('caffeine_12', 'CA7CAF~1'),
            ('caffeine_13', 'CAC6BA~1'),
            ('caffeine_14', 'CA11C5~1'),
            ('caffeine_15', 'CAFA24~1'),
            ('caffeine_16', 'CA453F~1'),
            ('caffeine_17', 'CA9F3A~1'),
            ('caffeine_18', 'CAE945~1'),
            ('caffeine_19', 'CA8AC9~1'),
            ('caffeine_20', 'CA3A41~1'),
            ('caffeine', 'CAFFEINE'),
        ],
        [
            ('caffeine_01', 'CAFFEI~1'),
            ('caffeine_02', 'CAFFEI~2'),
            ('caffeine_03', 'CAFFEI~3'),
            ('caffeine_04', 'CAFFEI~4'),
            ('CA517E~1', 'CA517E~1'),
            ('caffeine_05', 'CA517E~2'),
            ('CA7700~1', 'CA7700~1'),
            ('CA7700~2', 'CA7700~2'),
            ('CA7700~3', 'CA7700~3'),
            ('CA7700~4', 'CA7700~4'),
            ('CA7700~5', 'CA7700~5'),
            ('CA7700~6', 'CA7700~6'),
            ('CA7700~7', 'CA7700~7'),
            ('CA7700~8', 'CA7700~8'),
            ('CA7700~9', 'CA7700~9'),
            ('caffeine_06', 'CA770~10'),
        ],
    ],
)
def test__vfat_to_dos_filename_multiple(pairs):
    """Test DOS filename generation from VFAT filenames for non-empty directories.

    ``pairs`` is a list of ``(filename, dos_filename)`` tuples where ``dos_filename``
    is successively added to the list of existing DOS filenames in an imaginary
    directory.
    ``dos_filename`` is the DOS filename which is supposed to be assigned to the VFAT
    filename ``filename``.
    """
    existing_filenames: list[str] = []
    for filename, dos_filename in pairs:
        assert _vfat_to_dos_filename(filename, existing_filenames) == dos_filename
        existing_filenames.append(dos_filename)


@pytest.mark.parametrize(
    ['name_bytes', 'ext_bytes', 'expected'],
    [
        (b'_       ', b'   ', 0xFE),
        (b'C       ', b'   ', 0xFF),
        (b'C       ', b'C  ', 0xC8),
        (b'COFFEE  ', b'   ', 0xC4),
        (b'COFFEE  ', b'CAF', 0x43),
        (b'COFFEI~1', b'   ', 0xEC),
        (b'COFFEI~1', b'CAF', 0x2B),
        (b'#F0E3~1 ', b'!CO', 0x76),
        (b'_____~1 ', b'___', 0x53),
    ],
)
def test__dos_filename_checksum(name_bytes, ext_bytes, expected):
    """Test DOS filename checksum generation."""
    assert _dos_filename_checksum(name_bytes, ext_bytes) == expected


@pytest.mark.parametrize(
    ['dt', 'packed'],
    [
        (datetime(1980, 1, 1, 0, 0, 0), (33, 0, 0)),
        (datetime(1980, 1, 1, 0, 0, 1), (33, 0, 100)),
        (datetime(1980, 1, 1, 0, 0, 2), (33, 1, 0)),
        (datetime(1980, 1, 1, 0, 1, 0), (33, 32, 0)),
        (datetime(1980, 1, 1, 1, 0, 0), (33, 2048, 0)),
        (datetime(1980, 1, 2, 0, 0, 0), (34, 0, 0)),
        (datetime(1980, 2, 1, 0, 0, 0), (65, 0, 0)),
        (datetime(1981, 1, 1, 0, 0, 0), (545, 0, 0)),
        (datetime(1999, 12, 31, 23, 59, 58), (10143, 49021, 0)),
        (datetime(1999, 12, 31, 23, 59, 59), (10143, 49021, 100)),
        (datetime(2000, 1, 1, 0, 0, 0), (10273, 0, 0)),
        (datetime(2000, 1, 1, 0, 0, 1), (10273, 0, 100)),
        (datetime(2000, 1, 1, 0, 0, 2), (10273, 1, 0)),
        (datetime(2099, 12, 31, 23, 59, 58), (61343, 49021, 0)),
        (datetime(2099, 12, 31, 23, 59, 59), (61343, 49021, 100)),
        (datetime(2100, 1, 1, 0, 0, 0), (61473, 0, 0)),
        (datetime(2100, 1, 1, 0, 0, 1), (61473, 0, 100)),
        (datetime(2100, 1, 1, 0, 0, 2), (61473, 1, 0)),
        (datetime(2100, 1, 1, 0, 1, 0), (61473, 32, 0)),
        (datetime(2100, 1, 1, 1, 0, 0), (61473, 2048, 0)),
        (datetime(2100, 1, 2, 0, 0, 0), (61474, 0, 0)),
        (datetime(2107, 12, 31, 23, 59, 59), (65439, 49021, 100)),
    ],
)
def test_pack_unpack_dos_datetime(dt, packed):
    """Test successful DOS datetime packing and unpacking via ``pack_dos_datetime()``
    and ``unpack_dos_datetime()``.
    """
    assert pack_dos_datetime(dt) == packed
    assert unpack_dos_datetime(*packed) == dt


@pytest.mark.parametrize(
    ['dt', 'packed', 'unpacked'],
    [
        (datetime(1980, 1, 1, 0, 0, 0, 0), (33, 0, 0), datetime(1980, 1, 1, 0, 0, 0)),
        (datetime(1980, 1, 1, 0, 0, 0, 10), (33, 0, 0), datetime(1980, 1, 1, 0, 0, 0)),
        (
            datetime(1980, 1, 1, 0, 0, 0, 1000),
            (33, 0, 0),
            datetime(1980, 1, 1, 0, 0, 0),
        ),
        (
            datetime(1980, 1, 1, 0, 0, 0, 9999),
            (33, 0, 0),
            datetime(1980, 1, 1, 0, 0, 0),
        ),
        (
            datetime(1980, 1, 1, 0, 0, 0, 10000),
            (33, 0, 1),
            datetime(1980, 1, 1, 0, 0, 0, 10000),
        ),
        (
            datetime(1980, 1, 1, 0, 0, 0, 19999),
            (33, 0, 1),
            datetime(1980, 1, 1, 0, 0, 0, 10000),
        ),
        (
            datetime(1980, 1, 1, 0, 0, 0, 20000),
            (33, 0, 2),
            datetime(1980, 1, 1, 0, 0, 0, 20000),
        ),
    ],
)
def test_pack_dos_datetime_microseconds(dt, packed, unpacked):
    """Test DOS datetime packing and unpacking for datetimes including microseconds."""
    assert pack_dos_datetime(dt) == packed
    assert unpack_dos_datetime(*packed) == unpacked


@pytest.mark.parametrize(
    'dt',
    [
        datetime(1979, 1, 1, 0, 0, 0),
        datetime(1979, 12, 31, 23, 59, 59),
        datetime(2108, 1, 1, 0, 0, 0),
        datetime(2108, 12, 31, 23, 59, 59),
    ],
)
def test_pack_dos_datetime_fail_invalid_date(dt):
    """Test DOS datetime packing for invalid dates."""
    with pytest.raises(ValueError, match='Invalid DOS date.*'):
        pack_dos_datetime(dt)


@pytest.mark.parametrize(
    'packed',
    [
        (0, 0, 0),
        (0, 0, 100),
        (0, 1, 0),
        (0, 32, 0),
        (0, 33, 200),
        (0, 1920, 0),
        (0, 2048, 0),
        (0, 2080, 0),
        (0, 49152, 0),
        (0, 65535, 0),
        (1, 33, 0),
        (32, 33, 0),
        (417, 33, 0),
        (511, 33, 0),
        (544, 33, 0),
        (2048, 33, 0),
        (65535, 33, 0),
    ],
)
def test_unpack_dos_datetime_none(packed):
    """Test that ``unpack_dos_datetime()`` returns ``None`` for invalid packed
    datetimes.
    """
    assert unpack_dos_datetime(*packed) is None


EIGHT_DOT_THREE_ENTRY_EXAMPLE = EightDotThreeEntry(
    name=b'FILENAME',
    extension=b'EXT',
    _attributes=0,
    case_info_vfat=0,
    created_time_ten_ms=0,
    created_time=0,
    created_date=33,  # 1980-01-01
    last_accessed_date=33,
    _cluster_high_fat_32=0,
    last_modified_time=0,
    last_modified_date=33,
    _cluster=0,
    size=0,
)


class TestEightDotThreeEntry:
    """Tests for ``EightDotThreeEntry``."""

    @pytest.mark.parametrize(
        ['name', 'extension', 'case_info_vfat', 'filename', 'dos_filename'],
        [
            (b'A       ', b'B  ', 0, 'A.B', 'A.B'),
            (b'A       ', b'B  ', 8, 'a.B', 'A.B'),
            (b'A       ', b'B  ', 16, 'A.b', 'A.B'),
            (b'A       ', b'B  ', 24, 'a.b', 'A.B'),
            (b'FILENAME', b'   ', 0, 'FILENAME', 'FILENAME'),
            (b'FILENAME', b'   ', 8, 'filename', 'FILENAME'),
            (b'FILENAME', b'EXT', 0, 'FILENAME.EXT', 'FILENAME.EXT'),
            (b'FILENAME', b'EXT', 8, 'filename.EXT', 'FILENAME.EXT'),
            (b'FILENAME', b'EXT', 16, 'FILENAME.ext', 'FILENAME.EXT'),
            (b'FILENAME', b'EXT', 24, 'filename.ext', 'FILENAME.EXT'),
            (b'\x05ILENAME', b'EXT', 0, f'{E5}ILENAME.EXT', f'{E5}ILENAME.EXT'),
            (
                b'\x05ILENAME',
                b'EXT',
                24,
                f'{E5.lower()}ilename.ext',
                f'{E5}ILENAME.EXT',
            ),
            (b'(1234)6 ', b'{((', 24, '(1234)6.{((', '(1234)6.{(('),
        ],
    )
    def test_filename_and_hint(
        self, name, extension, case_info_vfat, filename, dos_filename
    ):
        """Test that ``filename()`` and the ``dos_filename`` property return the
        expected values.
        """
        entry = replace(
            EIGHT_DOT_THREE_ENTRY_EXAMPLE,
            name=name,
            extension=extension,
            case_info_vfat=case_info_vfat,
        )
        assert entry.filename(vfat=True) == filename
        assert entry.filename(vfat=False) == dos_filename
        assert entry.dos_filename == dos_filename

    @pytest.mark.parametrize(
        ['cluster_low', 'cluster_high', 'fat_32', 'cluster'],
        [
            (0, 0, False, 0),
            (0, 0, True, 0),
            (65535, 0, False, 65535),
            (65535, 0, True, 65535),
            (0, 1, False, 0),
            (0, 1, True, 65536),
            (1, 1, True, 65537),
            (0, 65535, False, 0),
            (0, 65535, True, 4294901760),
            (65535, 65535, False, 65535),
            (65535, 65535, True, 4294967295),
        ],
    )
    def test_cluster(self, cluster_low, cluster_high, fat_32, cluster):
        """Test that ``cluster()`` returns the correct cluster number based on the
        file system type and the raw cluster values.
        """
        entry = replace(
            EIGHT_DOT_THREE_ENTRY_EXAMPLE,
            _cluster=cluster_low,
            _cluster_high_fat_32=cluster_high,
        )
        assert entry.cluster(fat_32=fat_32) == cluster

    @pytest.mark.parametrize(
        ['name', 'hint'],
        [
            (b'FILENAME', None),
            (b'\x05ILENAME', None),
            (b'\xE5ILENAME', Hint.DELETED),
            (b'\x2E       ', Hint.DOT_ENTRY),
            (b'\x2E\x2E      ', Hint.DOT_ENTRY),
            (b'\x00' * 8, Hint.END_OF_ENTRIES),
        ],
    )
    def test_hint(self, name, hint):
        """Test that the ``hint`` property matches the expected value based on the
        raw filename.
        """
        entry = replace(EIGHT_DOT_THREE_ENTRY_EXAMPLE, name=name)
        assert entry.hint is hint

    @pytest.mark.parametrize(
        ['attributes_int', 'attributes', 'volume_label'],
        [
            (0, Attributes(0), False),
            (7, Attributes.READ_ONLY | Attributes.HIDDEN | Attributes.SYSTEM, False),
            (16, Attributes.SUBDIRECTORY, False),
            (15, Attributes.VFAT, False),
            (8, Attributes.VOLUME_LABEL, True),
        ],
    )
    def test_attributes(self, attributes_int, attributes, volume_label):
        """Test that the ``attributes`` and ``volume_label`` properties match the
        expected values.
        """
        entry = replace(EIGHT_DOT_THREE_ENTRY_EXAMPLE, _attributes=attributes_int)
        assert entry.attributes is attributes
        assert entry.volume_label is volume_label

    @pytest.mark.parametrize(
        ['date', 'time', 'time_ten_ms', 'created', 'last_accessed', 'last_modified'],
        [
            (0, 0, 0, None, None, None),
            (
                33,
                0,
                0,
                datetime(1980, 1, 1),
                datetime(1980, 1, 1),
                datetime(1980, 1, 1),
            ),
            (
                33,
                2081,
                0,
                datetime(1980, 1, 1, 1, 1, 2),
                datetime(1980, 1, 1),
                datetime(1980, 1, 1, 1, 1, 2),
            ),
            (
                33,
                2081,
                100,
                datetime(1980, 1, 1, 1, 1, 3),
                datetime(1980, 1, 1),
                datetime(1980, 1, 1, 1, 1, 2),
            ),
        ],
    )
    def test_datetime_properties(
        self, date, time, time_ten_ms, created, last_accessed, last_modified
    ):
        """Test that the ``created``, ``last_accessed``, and ``last_modified``
        properties match the expected datetimes.
        """
        entry = replace(
            EIGHT_DOT_THREE_ENTRY_EXAMPLE,
            created_time_ten_ms=time_ten_ms,
            created_time=time,
            created_date=date,
            last_accessed_date=date,
            last_modified_time=time,
            last_modified_date=date,
        )
        assert entry.created == created
        assert entry.last_accessed == last_accessed
        assert entry.last_modified == last_modified


VFAT_ENTRY_EXAMPLE = VfatEntry(
    seq=1,
    chars_1=b'ABCDEFGHIJ',
    attributes=15,
    type=0,
    checksum=0x12,  # not a real checksum
    chars_2=b'KLMNOPQRSTUV',
    cluster=0,
    chars_3=b'WXYZ',
)


class TestVfatEntry:
    """Tests for ``VfatEntry``."""

    @pytest.mark.parametrize(
        'replace_kwargs',
        [
            {'attributes': 15},
            {'attributes': 31},
            {'seq': 2},
            {'seq': 10},
            {'seq': 19},
            {'seq': 20},
            {'seq': 32 + 20},
            {'seq': 128 + 20},
        ],
    )
    def test_validate_success(self, replace_kwargs):
        """Test custom validation logic for succeeding cases."""
        replace(VFAT_ENTRY_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['replace_kwargs', 'msg_contains'],
        [
            ({'attributes': 0}, 'Invalid attributes'),
            ({'attributes': 1}, 'Invalid attributes'),
            ({'attributes': 3}, 'Invalid attributes'),
            ({'attributes': 7}, 'Invalid attributes'),
            ({'attributes': 14}, 'Invalid attributes'),
            ({'attributes': 16}, 'Invalid attributes'),
            ({'seq': 0}, 'Sequence number'),
            ({'seq': 21}, 'Sequence number'),
            ({'seq': 22}, 'Sequence number'),
            ({'seq': 32}, 'Sequence number'),
            ({'seq': 64}, 'Sequence number'),
            ({'seq': 128 + 22}, 'Sequence number'),
            ({'cluster': 1}, 'Cluster number'),
            ({'cluster': 2}, 'Cluster number'),
        ],
    )
    def test_validate_fail(self, replace_kwargs, msg_contains):
        """Test custom validation logic for failing cases."""
        with pytest.raises(ValidationError, match=f'.*{msg_contains}.*'):
            replace(VFAT_ENTRY_EXAMPLE, **replace_kwargs)

    @pytest.mark.parametrize(
        ['seq', 'first_lfn_entry', 'number'],
        [
            (1, False, 1),
            (20, False, 20),
            (33, False, 1),
            (65, True, 1),
            (84, True, 20),
            (211, True, 19),
        ],
    )
    def test_properties(self, seq, first_lfn_entry, number):
        """Test that properties defined on VFAT entries match the expected values."""
        entry = replace(VFAT_ENTRY_EXAMPLE, seq=seq)
        assert entry.first_lfn_entry == first_lfn_entry
        assert entry.number == number
