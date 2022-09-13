"""Byte struct packing and validation."""

from __future__ import annotations

import struct
import sys
from dataclasses import InitVar
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Type, TypeVar

from typing_extensions import Annotated, get_args, get_origin, get_type_hints

from .base import ValidationError
from .typing import NoneType

if TYPE_CHECKING:
    from .volume import Volume

__all__ = ['ByteStruct', 'BYTE_ORDERS']


# Workaround for python/cpython#88962:
# Starting with Python 3.11, InitVar does not need to be callable anymore to be used
# within a lazily evaluated type annotation.
if sys.version_info < (3, 11):
    InitVar.__call__ = lambda *args, **kwargs: None  # type: ignore[assignment]


BYTE_ORDERS = ('<', '>', '!', '=')
SIGNED_SPECIFIERS = ('signed', 'unsigned')
INT_CONVERSION = {1: 'B', 2: 'H', 4: 'I', 8: 'Q'}
FLOAT_CONVERSION = {2: 'e', 4: 'f', 8: 'd'}

_BsType = TypeVar('_BsType')
_Bs = TypeVar('_Bs', bound='ByteStruct')


class _ByteStructMeta(type):
    """Metaclass for ``ByteStruct``."""

    def __init__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        byteorder: Literal['<', '>', '!', '='] = '<',
    ):
        super().__init__(name, bases, namespace)
        if not bases:
            return  # cls is ByteStruct

        type_hints = get_type_hints(cls, include_extras=True)
        format_ = f'{byteorder}'
        fields = {}

        for name, type_ in type_hints.items():
            origin = get_origin(type_)

            if origin is ClassVar or type(type_) is InitVar:
                continue

            if origin is Annotated:
                # Annotated
                args = get_args(type_)
                annotated_type = args[0]
                size = args[1]
                if not isinstance(size, int):
                    raise TypeError('Field size must be specified as int')
                if size < 1:
                    raise ValueError('Field size must be greater than or equal to 1')

                if annotated_type is int:
                    signed = False
                    if len(args) > 2:
                        if args[2] not in SIGNED_SPECIFIERS:
                            raise TypeError(
                                f'Invalid specifier {args[2]} on field {name!r}, must '
                                f'be one of {SIGNED_SPECIFIERS}'
                            )
                        signed = args[2] == 'signed'
                    if size not in INT_CONVERSION.keys():
                        raise ValueError(
                            f'Invalid int field size {size}, must be one of '
                            f'{tuple(INT_CONVERSION.keys())}'
                        )
                    format_specifier = INT_CONVERSION[size]
                    if signed:
                        format_specifier = format_specifier.lower()
                    format_ += format_specifier

                elif annotated_type is float:
                    if size not in FLOAT_CONVERSION.keys():
                        raise ValueError(
                            f'Invalid float field size {size}, must be one of '
                            f'{tuple(FLOAT_CONVERSION.keys())}'
                        )
                    format_ += FLOAT_CONVERSION[size]

                elif annotated_type is bytes:
                    format_ += f'{size}s'
                elif annotated_type is NoneType:
                    format_ += f'{size}x'  # pad bytes
                else:
                    raise TypeError(
                        f'Annotated type {args[0]} of field {name!r} is not allowed '
                        f'for {cls.__name__}'
                    )

            elif isinstance(type_, cls.__class__):
                # embedded ByteStruct, treat as bytes
                format_ += f'{len(type_)}s'

            else:
                raise TypeError(
                    f'Type {type_} of field {name!r} is not allowed for {cls.__name__}'
                )

            fields[name] = type_

        cls.__bytestruct_fields__ = fields
        cls.__bytestruct_format__ = format_
        cls.__bytestruct_size__ = struct.calcsize(format_)

        # Add skip_annotation_validation as an optional parameter to be able to skip
        # execution of validate_against_annotations().
        annotations_ = namespace.get('__annotations__', {})
        annotations_['skip_annotation_validation'] = InitVar[bool]  # type hint
        namespace['__annotations__'] = annotations_
        cls.skip_annotation_validation = False  # default value

    def __len__(cls) -> int:
        return cls.__bytestruct_size__

    def from_bytes(cls: Type[_BsType], b: bytes) -> _BsType:
        raise NotImplementedError


class ByteStruct(metaclass=_ByteStructMeta):
    """Byte structure."""

    # We exclude these dunder attributes from bytestruct via ClassVar, but they will
    # still be accessible from the instance.
    __bytestruct_fields__: ClassVar['dict[str, Any]']
    __bytestruct_format__: ClassVar[str]
    __bytestruct_size__: ClassVar[int]

    def validate_against_annotations(self) -> None:
        """Validate against field annotations.

        Automatically executed after object creation.
        """
        for name, type_ in self.__bytestruct_fields__.items():
            value = getattr(self, name)
            origin = get_origin(type_)

            if origin is Annotated:
                # Annotated
                args = get_args(type_)
                annotated_type = args[0]
                size = args[1]

                # noinspection PyTypeHints
                if not isinstance(value, annotated_type):
                    raise TypeError(
                        f'Value {value!r} of field {name!r} is not of specified type '
                        f'{annotated_type}'
                    )

                if annotated_type is int:
                    signed = False
                    if len(args) > 2:
                        signed = args[2] == 'signed'

                    if signed:
                        half = 1 << (size * 8 - 1)
                        min_ = -half
                        max_ = half - 1
                    else:
                        min_ = 0
                        max_ = (1 << size * 8) - 1

                    if not min_ <= value <= max_:
                        raise ValidationError(
                            f'Value {value} of field {name!r} must be in range '
                            f'({min_}, {max_})'
                        )

                elif annotated_type is bytes:
                    if len(value) != size:
                        raise ValidationError(
                            f'Value of field {name!r} must be of length {size} bytes, '
                            f'got {len(value)} bytes'
                        )

            elif isinstance(type_, _ByteStructMeta):
                # embedded ByteStruct
                if not isinstance(value, type_):
                    raise TypeError(
                        f'Value {value!r} of field {name!r} is not of specified type '
                        f'{type_}'
                    )

    def validate(self) -> None:
        """Custom validation.

        Automatically executed after object creation, but after field annotation
        validation.
        """

    def validate_for_volume(self, volume: Volume, *, recurse: bool = False) -> None:
        """Custom validation to check suitability for a specific volume.

        If ``recurse`` is set, ``validate_for_volume`` is also called on every
        embedded ``ByteStruct`` with ``recurse`` set.
        """
        if recurse:
            for name, type_ in self.__bytestruct_fields__.items():
                if isinstance(type_, _ByteStructMeta):
                    value = getattr(self, name)
                    value.validate_for_volume(volume, recurse=True)

    # noinspection PyUnusedLocal
    def __init__(
        self, *args: Any, skip_annotation_validation: bool = False, **kwargs: Any
    ):
        # dataclass might replace __init__, so call __post_init__ if we're not replaced.
        self.__post_init__(skip_annotation_validation)

    def __post_init__(self, skip_annotation_validation: bool) -> None:
        if not skip_annotation_validation:
            self.validate_against_annotations()
        self.validate()

    @classmethod
    def from_bytes(cls: Type[_Bs], b: bytes) -> _Bs:
        """Parse structure from ``bytes``."""
        fields = cls.__bytestruct_fields__
        size = cls.__bytestruct_size__

        if len(b) != size:
            raise ValueError(f'Structure must be {size} bytes long, got {len(b)} bytes')

        unpacked_values = struct.unpack(cls.__bytestruct_format__, b)
        values: list[Any] = []

        # create embedded bytestructs
        for type_, unpacked_value in zip(fields.values(), unpacked_values):
            if isinstance(type_, _ByteStructMeta):
                values.append(type_.from_bytes(unpacked_value))
            else:
                values.append(unpacked_value)

        return cls(*values, skip_annotation_validation=True)

    def __bytes__(self) -> bytes:
        values = []
        for name in self.__bytestruct_fields__.keys():
            value = getattr(self, name)
            if isinstance(value, ByteStruct):
                value = bytes(value)
            values.append(value)

        return struct.pack(self.__bytestruct_format__, *values)

    def __len__(self) -> int:
        return self.__bytestruct_size__
