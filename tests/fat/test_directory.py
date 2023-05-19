"""Tests for the ``directory`` module of the ``fat`` package."""

import pytest

from diskfs.base import ValidationError

# noinspection PyProtectedMember
from diskfs.fat.directory import (
    CASE_INFO_EXT_LOWER,
    CASE_INFO_NAME_LOWER,
    DOS_FILENAME_OEM_ENCODING,
    _check_vfat_filename,
    _get_case_info,
    _is_valid_dos_filename,
    _is_valid_vfat_filename,
    _pack_dos_filename,
    _requires_vfat,
    _split_filename,
    _to_be_saved_as_vfat,
    _unpack_dos_filename,
)

DOS_ENC = DOS_FILENAME_OEM_ENCODING
E5 = b'\xE5'.decode(DOS_ENC)


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
