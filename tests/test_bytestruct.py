"""Tests for the ``bytestruct`` module."""

import sys
from dataclasses import InitVar, dataclass
from functools import lru_cache
from itertools import chain
from math import isnan
from typing import ClassVar

import pytest
from typing_extensions import Annotated, get_args

from diskfs.base import ValidationError
from diskfs.bytestruct import ByteStruct


class ArbitraryClass:
    """A class."""


@dataclass(frozen=True)
class EmptyByteStruct(ByteStruct):
    """``ByteStruct`` without any fields."""

    pass


@dataclass(frozen=True)
class ArbitraryByteStruct(ByteStruct):
    """A valid ``ByteStruct`` subclass."""

    field: Annotated[int, 4, 'signed']


@dataclass(frozen=True)
class CustomValidationByteStruct(ByteStruct):
    f_1: Annotated[int, 4]
    f_2: Annotated[bytes, 2]

    def validate(self) -> None:
        if self.f_1 <= 100:
            raise ValidationError('f_1 must be greater than 100')
        if self.f_2 != b'\xAB\x34':
            raise ValidationError(r"f_2 must be b'\xAB\x34'")


BYTEORDER_IN_WORDS = {'<': 'little', '>': 'big', '!': 'big', '=': sys.byteorder}


@lru_cache
def bytestruct_single_int(byteorder: str, size: int, signed: bool):
    """Return a ``ByteStruct`` with byte order ``byteorder`` defining a single field
    of type ``int`` annotated with ``size`` and ``'signed'`` or ``'unsigned'``
    depending on the value of ``signed``.
    """
    signed_specifier = 'signed' if signed else 'unsigned'

    @dataclass(frozen=True)
    class B(ByteStruct, byteorder=byteorder):
        field: Annotated[int, size, signed_specifier]

    return B


@lru_cache
def bytestruct_single_float(byteorder: str, size: int):
    """Return a ``ByteStruct`` with byte order ``byteorder`` defining a single field
    of type ``float`` and size ``size``.
    """

    @dataclass(frozen=True)
    class B(ByteStruct, byteorder=byteorder):
        field: Annotated[float, size]

    return B


@lru_cache
def bytestruct_single_bytes(byteorder: str, size: int):
    """Return a ``ByteStruct`` with byte order ``byteorder`` defining a single field
    of type ``bytes`` and size ``size``.
    """

    @dataclass(frozen=True)
    class B(ByteStruct, byteorder=byteorder):
        field: Annotated[bytes, size]

    return B


@lru_cache
def bytestruct_single_none(byteorder: str, size: int):
    """Return a ``ByteStruct`` with byte order ``byteorder`` defining a single field
    of type ``None`` and size ``size``.
    """

    @dataclass(frozen=True)
    class B(ByteStruct, byteorder=byteorder):
        field: Annotated[None, size]

    return B


@lru_cache
def bytestruct_multi(byteorder: str):
    """Return a typical ``ByteStruct`` with byte order ``byteorder`` defining multiple
    fields.
    """

    @dataclass(frozen=True)
    class B(ByteStruct, byteorder=byteorder):
        f_1: Annotated[int, 2]
        f_2: Annotated[None, 3]
        f_3: ArbitraryByteStruct
        f_4: Annotated[int, 4, 'signed']
        f_5: Annotated[None, 5]
        f_6: Annotated[bytes, 10]
        f_7: EmptyByteStruct
        f_8: Annotated[float, 4]

        expected_format: ClassVar[
            str
        ] = f'{byteorder}H3x{len(ArbitraryByteStruct)}si5x10s0sf'

    return B


class TestByteStructMeta:
    """Tests for ``_ByteStructMeta``."""

    @pytest.mark.parametrize(
        'annotation', [dict, str, int, float, bytes, None, ArbitraryClass, ByteStruct]
    )
    def test_analysis_fail_unannotated(self, annotation):
        """Test that analysis fails when a field has an unannotated type which is not
        a subclass of ``ByteStruct``.
        """
        with pytest.raises(TypeError, match='Unannotated type.*'):
            # noinspection PyUnusedLocal
            @dataclass(frozen=True)
            class B(ByteStruct):
                field: annotation  # type: ignore[valid-type]

    @pytest.mark.parametrize(
        'annotated_type', [dict, str, ByteStruct, ArbitraryByteStruct]
    )
    def test_analysis_fail_annotated_unsupported(self, annotated_type):
        """Test that analysis fails when a field has an annotated type which is
        unsupported.
        """
        with pytest.raises(TypeError, match='Annotated type.*'):
            # noinspection PyUnusedLocal
            @dataclass(frozen=True)
            class B(ByteStruct):
                field: Annotated[annotated_type, 4]  # type: ignore[valid-type]

    @pytest.mark.parametrize('type_', [int, float, bytes, None])
    @pytest.mark.parametrize('size', [(), '1', '4', 0, -10, 4.1])
    def test_analysis_fail_annotated_size(self, type_, size):
        """Test that analysis fails when a field has a type annotated with an invalid
        field size.
        """
        # noinspection PyTypeChecker
        with pytest.raises((TypeError, ValueError), match='.*(f|F)ield size.*'):
            # noinspection PyUnusedLocal
            @dataclass(frozen=True)
            class B(ByteStruct):
                field: Annotated[type_, size]  # type: ignore[valid-type]

    @pytest.mark.parametrize('type_', [int, float])
    @pytest.mark.parametrize('size', [3, 5, 6, 7, 9, 16])
    def test_analysis_fail_annotated_size_int_float(self, type_, size):
        """Test that analysis fails when a field has an ``int`` or ``float`` type
        annotated with a field size specifically invalid for both of these types.
        """
        with pytest.raises(ValueError, match='.*(f|F)ield size.*'):
            # noinspection PyUnusedLocal
            @dataclass(frozen=True)
            class B(ByteStruct):
                field: Annotated[type_, size]  # type: ignore[valid-type]

    @pytest.mark.parametrize(
        'annotation',
        [Annotated[float, 1], Annotated[int, 4, 'test'], Annotated[int, 4, 10]],
    )
    def test_analysis_fail_annotated_specific(self, annotation):
        """Test that analysis fails when a field has a type annotated with specific
        combinations of values invalid for that type.
        """
        with pytest.raises(ValueError):
            # noinspection PyUnusedLocal
            @dataclass(frozen=True)
            class B(ByteStruct):
                field: annotation  # type: ignore[valid-type]

    @pytest.mark.parametrize(
        ['type_', 'size', 'signed', 'expected_format', 'expected_size'],
        [
            (int, 1, None, 'B', 1),
            (int, 1, 'unsigned', 'B', 1),
            (int, 1, 'signed', 'b', 1),
            (int, 2, None, 'H', 2),
            (int, 2, 'signed', 'h', 2),
            (int, 4, None, 'I', 4),
            (int, 4, 'signed', 'i', 4),
            (int, 8, None, 'Q', 8),
            (int, 8, 'signed', 'q', 8),
            (float, 2, None, 'e', 2),
            (float, 4, None, 'f', 4),
            (float, 8, None, 'd', 8),
            (bytes, 1, None, '1s', 1),
            (bytes, 3, None, '3s', 3),
            (bytes, 10, None, '10s', 10),
            (None, 1, None, '1x', 1),
            (None, 3, None, '3x', 3),
            (None, 10, None, '10x', 10),
        ],
    )
    def test_analysis_success_annotated(
        self, type_, size, signed, expected_format, expected_size
    ):
        """Test the result of the analysis of different valid annotated field types."""
        if signed is None:
            annotation = Annotated[type_, size]  # type: ignore[valid-type]
        else:
            annotation = Annotated[type_, size, signed]  # type: ignore[misc]

        @dataclass(frozen=True)
        class B(ByteStruct):
            field: annotation

        assert len(B.__bytestruct_fields__) == 1
        descriptor = B.__bytestruct_fields__['field']
        annotated_args = get_args(annotation)
        assert descriptor.type_origin == annotated_args[0]
        assert descriptor.type_args == annotated_args[1:]
        assert not descriptor.is_bytestruct

        assert B.__bytestruct_format__ == '<' + expected_format
        assert B.__bytestruct_size__ == expected_size

    def test_analysis_success_embedded(self):
        """Test the result of the analysis of a field representing an embedded
        ``ByteStruct``."""

        @dataclass(frozen=True)
        class B(ByteStruct):
            field: ArbitraryByteStruct

        assert B.__bytestruct_fields__ == {'field': (ArbitraryByteStruct, (), True)}
        assert B.__bytestruct_format__ == f'<{len(ArbitraryByteStruct)}s'
        assert B.__bytestruct_size__ == len(ArbitraryByteStruct)

    @pytest.mark.parametrize('byteorder', BYTEORDER_IN_WORDS.keys())
    def test_analysis_success_multiple(self, byteorder):
        """Test the result of the analysis of a typical ``ByteStruct`` with multiple
        fields.
        """
        bs = bytestruct_multi(byteorder)
        assert len(bs.__bytestruct_fields__) == 8
        assert bs.__bytestruct_format__ == bs.expected_format
        assert bs.__bytestruct_size__ == len(ArbitraryByteStruct) + 28

    def test_analysis_success_empty(self):
        """Test the result of the analysis of a ``ByteStruct`` without any fields."""
        assert EmptyByteStruct.__bytestruct_fields__ == {}
        assert len(EmptyByteStruct.__bytestruct_format__) == 1  # only byte order
        assert EmptyByteStruct.__bytestruct_size__ == 0
        assert len(EmptyByteStruct) == 0

    def test_classvar(self):
        """Test that the analysis excludes fields annotated as ``ClassVar`` and that
        such fields are usable as usual in the context of the dataclass.
        """

        @dataclass(frozen=True)
        class B(ByteStruct):
            f_1: Annotated[int, 1]
            cv: ClassVar[str] = 'test'
            f_2: Annotated[bytes, 3]

        assert len(B.__bytestruct_fields__) == 2
        assert B.__bytestruct_format__ == '<B3s'
        assert B.__bytestruct_size__ == 4
        assert B.cv == 'test'

    def test_initvar(self):
        """Test that the analysis excludes fields annotated as ``InitVar`` and that
        such a field is usable as usual in the context of the dataclass.
        """

        @dataclass(frozen=True)
        class B(ByteStruct):
            f_1: Annotated[bytes, 3]
            iv: InitVar[str]
            f_2: Annotated[int, 1, 'signed']

            def __post_init__(self, iv: str) -> None:  # type: ignore[override]
                super().__post_init__()
                assert iv == 'test'

        assert len(B.__bytestruct_fields__) == 2
        assert B.__bytestruct_format__ == '<3sb'
        assert B.__bytestruct_size__ == 4
        B(b'xyz', 'test', 5)


class TestByteStruct:
    """Tests for ``ByteStruct``."""

    def test_instantiation_fail_direct(self):
        """Test that creating a direct instance of ``ByteStruct`` fails."""
        with pytest.raises(TypeError, match='.*direct.*'):
            ByteStruct()
        with pytest.raises(TypeError, match='.*direct.*'):
            ByteStruct(1, 'a', b='c')
        with pytest.raises(TypeError, match='.*direct.*'):
            ByteStruct.from_bytes(b'')

    def test_instantiation_fail_not_a_dataclass(self):
        """Test that the instantiation of a ``ByteStruct`` fails if the according
        ``ByteStruct`` subclass is not a ``dataclass``.
        """

        class B(ByteStruct):
            field: Annotated[int, 4]

        with pytest.raises(TypeError, match='.*dataclass.*'):
            B(2)
        with pytest.raises(TypeError, match='.*dataclass.*'):
            B.from_bytes(b'\x98\x76\x54\x32')

    def test_instantiation_fail_not_a_frozen_dataclass(self):
        """Test that the instantiation of a ``ByteStruct`` fails if the according
        ``ByteStruct`` subclass is not a frozen ``dataclass``.
        """

        @dataclass
        class B(ByteStruct):
            field: Annotated[int, 4]

        with pytest.raises(TypeError, match='.*frozen.*'):
            B(2)
        with pytest.raises(TypeError, match='.*frozen.*'):
            B.from_bytes(b'\x98\x76\x54\x32')

    @pytest.mark.parametrize('bytes_', [b'', b'a', b'abcdef'])
    def test_from_bytes_fail_length(self, bytes_):
        """Test that ``from_bytes()`` fails if it is supplied with a ``bytes``
        argument of wrong length.
        """
        with pytest.raises(ValueError, match='.*long.*'):
            ArbitraryByteStruct.from_bytes(bytes_)

    def test_empty(self):
        """Test validation and conversion on a ``ByteStruct`` subclass without any
        fields.
        """
        bs_from_values = EmptyByteStruct()
        assert bytes(bs_from_values) == b''
        assert len(bs_from_values) == 0

        bs_from_bytes = EmptyByteStruct.from_bytes(b'')
        assert len(bs_from_bytes) == 0

    @pytest.mark.parametrize(
        ['byteorder', 'size', 'signed', 'value', 'accept'],
        chain.from_iterable(
            (
                (byteorder, size, False, -1, False),
                (byteorder, size, False, 0, True),
                (byteorder, size, False, 1 << (size * 8 - 1), True),
                (byteorder, size, False, (1 << size * 8) - 1, True),
                (byteorder, size, False, (1 << size * 8), False),
                (byteorder, size, True, -(1 << size * 8 - 1) - 1, False),
                (byteorder, size, True, -(1 << size * 8 - 1), True),
                (byteorder, size, True, -1, True),
                (byteorder, size, True, 0, True),
                (byteorder, size, True, 1, True),
                (byteorder, size, True, (1 << size * 8 - 1) - 1, True),
                (byteorder, size, True, (1 << size * 8 - 1), False),
            )
            for byteorder in ['<', '>', '!', '=']
            for size in [1, 2, 4, 8]
        ),
    )
    def test_single_int(self, byteorder, size, signed, value, accept):
        """Test validation and conversion on a ``ByteStruct`` subclass with a single
        ``int`` field.
        """
        bs = bytestruct_single_int(byteorder, size, signed)
        if accept:
            byteorder_in_words = BYTEORDER_IN_WORDS[byteorder]
            expected_bytes = value.to_bytes(len(bs), byteorder_in_words, signed=signed)

            bs_from_values = bs(value)
            assert bytes(bs_from_values) == expected_bytes
            assert len(bs_from_values) == size

            bs_from_bytes = bs.from_bytes(expected_bytes)
            assert bs_from_bytes.field == value
            assert len(bs_from_bytes) == size

        else:
            with pytest.raises(ValidationError):
                bs(value)

    @pytest.mark.parametrize('byteorder', BYTEORDER_IN_WORDS.keys())
    @pytest.mark.parametrize('size', [2, 4, 8])
    @pytest.mark.parametrize(
        'value', [-10.6, 0, 2, 8.7, float('inf'), float('-inf'), float('nan')]
    )
    def test_single_float(self, byteorder, size, value):
        """Test validation and conversion on a ``ByteStruct`` subclass with a single
        ``float`` field.
        """
        bs = bytestruct_single_float(byteorder, size)
        bs_from_values = bs(value)
        bs_bytes = bytes(bs_from_values)
        bs_from_bytes = bs.from_bytes(bs_bytes)

        assert len(bs_from_values) == size
        assert len(bs_bytes) == size
        assert len(bs_from_bytes) == size
        assert isnan(value) or value - 0.1 <= bs_from_bytes.field <= value + 0.1

    @pytest.mark.parametrize('byteorder', BYTEORDER_IN_WORDS.keys())
    @pytest.mark.parametrize(
        ['size', 'value', 'accept'],
        [
            (1, b'', False),
            (1, b'a', True),
            (1, b'ab', False),
            (3, b'abc', True),
            (3, b'abcd', False),
            (11, b'abcdefghijk', True),
        ],
    )
    def test_single_bytes(self, byteorder, size, value, accept):
        """Test validation and conversion on a ``ByteStruct`` subclass with a single
        ``bytes`` field.
        """
        bs = bytestruct_single_bytes(byteorder, size)
        if accept:
            bs_from_values = bs(value)
            assert bytes(bs_from_values) == value
            assert len(bs_from_values) == size

            bs_from_bytes = bs.from_bytes(value)
            assert bs_from_bytes.field == value
            assert len(bs_from_bytes) == size
        else:
            with pytest.raises(ValidationError):
                bs(value)

    @pytest.mark.parametrize('byteorder', BYTEORDER_IN_WORDS.keys())
    @pytest.mark.parametrize('size', [3, 10])
    def test_single_none(self, byteorder, size):
        """Test validation and conversion on a ``ByteStruct`` subclass with a single
        ``None`` (pad bytes) field.
        """
        bs = bytestruct_single_none(byteorder, size)
        expected_bytes = size * b'\x00'

        bs_from_values = bs(None)
        assert bytes(bs_from_values) == expected_bytes
        assert len(bs_from_values) == size

        bs_from_bytes = bs.from_bytes(expected_bytes)
        assert bs_from_bytes.field is None
        assert len(bs_from_bytes) == size

    @pytest.mark.parametrize('byteorder', BYTEORDER_IN_WORDS.keys())
    def test_single_embedded(self, byteorder):
        """Test validation and conversion on a ``ByteStruct`` subclass with a single
        ``None`` (pad bytes) field.
        """

        @dataclass(frozen=True)
        class B(ByteStruct):
            field: ArbitraryByteStruct

        value = ArbitraryByteStruct(24)
        expected_bytes = bytes(value)

        bs_from_values = B(value)
        assert bytes(bs_from_values) == expected_bytes
        assert len(bs_from_values) == len(value)

        bs_from_bytes = B.from_bytes(expected_bytes)
        assert bs_from_bytes.field == value
        assert len(bs_from_bytes) == len(value)

    @pytest.mark.parametrize('byteorder', BYTEORDER_IN_WORDS.keys())
    @pytest.mark.parametrize(
        'values',
        [
            (
                -1,
                None,
                ArbitraryByteStruct(4),
                -5,
                None,
                b'abcdefghij',
                EmptyByteStruct(),
                1.4,
            ),
            (
                10,
                None,
                ArbitraryByteStruct(4),
                -5,
                None,
                b'abcdefgh',
                EmptyByteStruct(),
                1.4,
            ),
            (
                -1,
                None,
                ArbitraryByteStruct(4),
                -5,
                None,
                b'abcdefgh',
                EmptyByteStruct(),
                1.4,
            ),
        ],
    )
    def test_multiple_fail(self, byteorder, values):
        """Test that validation fails on a typical ``ByteStruct`` with multiple fields
        if the validation of at least one value fails.
        """
        bs = bytestruct_multi(byteorder)
        with pytest.raises(ValidationError):
            # noinspection PyArgumentList
            bs(*values)

    @pytest.mark.parametrize(
        ['byteorder', 'expected_bytes'],
        [
            (
                '<',
                b'\n\x00\x00\x00\x00\x04\x00\x00\x00\xfb\xff\xff\xff\x00\x00\x00\x00'
                b'\x00abcdefghij33\xb3?',
            ),
            (
                '>',
                b'\x00\n\x00\x00\x00\x04\x00\x00\x00\xff\xff\xff\xfb\x00\x00\x00\x00'
                b'\x00abcdefghij?\xb333',
            ),
        ],
    )
    def test_multiple_success(self, byteorder, expected_bytes):
        """Test succeeding validation and conversion on a typical ``ByteStruct`` with
        multiple fields.
        """
        bs = bytestruct_multi(byteorder)
        values = (
            10,
            None,
            ArbitraryByteStruct(4),
            -5,
            None,
            b'abcdefghij',
            EmptyByteStruct(),
            1.4,
        )

        bs_from_values = bs(*values)
        assert bytes(bs_from_values) == expected_bytes
        assert len(bs_from_values) == len(bs)

        bs_from_bytes = bs.from_bytes(expected_bytes)
        assert bs_from_bytes.f_1 == values[0]
        assert bs_from_bytes.f_2 is None
        assert bs_from_bytes.f_4 == values[3]
        assert bs_from_bytes.f_5 is None
        assert bs_from_bytes.f_6 == values[5]
        assert values[7] - 0.1 <= bs_from_bytes.f_8 <= values[7] + 0.1
        assert len(bs_from_bytes) == len(bs)

    @pytest.mark.parametrize(
        ['value_1', 'value_2', 'accept'],
        [
            (420, b'\xAB\x34', True),
            (90, b'\xAB\x34', False),
            (420, b'\xBB\x34', False),
            (90, b'\xBB\x34', False),
        ],
    )
    def test_custom_validation(self, value_1, value_2, accept):
        """Test that custom validation logic is automatically triggered when
        instantiating a ``ByteStruct``.
        """
        if accept:
            CustomValidationByteStruct(value_1, value_2)
        else:
            with pytest.raises(Exception):
                CustomValidationByteStruct(value_1, value_2)
