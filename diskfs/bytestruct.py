"""Byte struct packing and validation."""

from __future__ import annotations

import struct
from dataclasses import InitVar, is_dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NamedTuple, TypeVar

from typing_extensions import Annotated, get_args, get_origin, get_type_hints

from .base import ValidationError
from .typing import NoneType

if TYPE_CHECKING:
    from .volume import Volume

__all__ = ['ByteStruct', 'BYTE_ORDERS']


BYTE_ORDERS = ('<', '>', '!', '=')
SIGNED_SPECIFIERS = ('signed', 'unsigned')
INT_CONVERSION = {1: 'B', 2: 'H', 4: 'I', 8: 'Q'}
FLOAT_CONVERSION = {2: 'e', 4: 'f', 8: 'd'}
INTERNAL_NAMES = (
    '__bytestruct_fields__',
    '__bytestruct_format__',
    '__bytestruct_size__',
    '__bytestruct_cached__',
)

_BsType = TypeVar('_BsType')
_Bs = TypeVar('_Bs', bound='ByteStruct')


class _FieldDescriptor(NamedTuple):

    type_origin: Any
    type_args: tuple[Any, ...] = ()
    is_bytestruct: bool = False


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
            if name in INTERNAL_NAMES or type(type_) is InitVar:
                continue

            origin = get_origin(type_)
            if origin is ClassVar:
                continue

            # Embedded ByteStruct, treat as bytes
            if isinstance(type_, cls.__class__):
                format_ += f'{len(type_)}s'
                fields[name] = _FieldDescriptor(type_, is_bytestruct=True)
                continue

            if origin is not Annotated:
                raise TypeError(
                    f'Type {type_} of field {name!r} is not allowed for ByteStruct'
                )

            # Annotated type
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
                            f'Invalid specifier {args[2]} on field {name!r}, must be '
                            f'one of {SIGNED_SPECIFIERS}'
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
                    f'Annotated type {args[0]} of field {name!r} is not allowed for '
                    f'ByteStruct'
                )

            fields[name] = _FieldDescriptor(annotated_type, args[1:])

        cls.__bytestruct_fields__ = fields
        cls.__bytestruct_format__ = format_
        cls.__bytestruct_size__ = struct.calcsize(format_)

    def __len__(cls) -> int:
        return cls.__bytestruct_size__

    def from_bytes(cls: type[_BsType], b: bytes) -> _BsType:
        raise NotImplementedError


class ByteStruct(metaclass=_ByteStructMeta):
    """Byte structure."""

    # Populated per class
    __bytestruct_fields__: 'dict[str, _FieldDescriptor]'
    __bytestruct_format__: str
    __bytestruct_size__: int

    # Populated per instance
    __bytestruct_cached__: bytes

    # noinspection PyUnusedLocal
    def __init__(self, *args: Any, **kwargs: Any):
        cls = self.__class__
        if cls.__bases__ == (object,):
            raise TypeError(f'Cannot directly instantiate {cls.__name__}')
        if not is_dataclass(cls):
            raise TypeError('ByteStruct subclass must be a frozen dataclass')

    def __post_init__(self) -> None:
        try:
            params = getattr(self.__class__, '__dataclass_params__')
        except AttributeError:
            raise
        else:
            if not params.frozen:
                raise TypeError('ByteStruct subclass must be a frozen dataclass')

        if not hasattr(self, '__bytestruct_cached__'):
            self._validate_and_cache()
        self.validate()

    def _validate_and_cache(self) -> None:
        values = []

        for name, descriptor in self.__bytestruct_fields__.items():
            type_ = descriptor.type_origin

            # struct.pack() does not expect a value for pad bytes, so skip
            if type_ is NoneType:
                continue

            value = getattr(self, name)

            # The embedded ByteStruct was validated the same way, so we can simply
            # request its cached bytes version without a huge performance impact.
            if descriptor.is_bytestruct:
                values.append(bytes(value))
                continue

            # Annotated type
            if type_ is bytes:
                size = descriptor.type_args[0]
                if len(value) != size:
                    raise ValidationError(
                        f'Value of field {name!r} must be of length {size} bytes, '
                        f'got {len(value)} bytes'
                    )

            values.append(value)

        # All other values (int and float) are validated via struct.pack().
        try:
            bytes_ = struct.pack(self.__bytestruct_format__, *values)
        except (struct.error, OverflowError) as e:
            raise ValidationError(
                f'Value out of range (format is {self.__bytestruct_format__!r})'
            ) from e

        # Keep packed version of ByteStruct in memory
        # Avoid __setattr__() here because this is a frozen dataclass.
        self.__dict__['__bytestruct_cached__'] = bytes_

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
            for name, descriptor in self.__bytestruct_fields__.items():
                if descriptor.is_bytestruct:
                    value = getattr(self, name)
                    value.validate_for_volume(volume, recurse=True)

    @classmethod
    def from_bytes(cls: type[_Bs], b: bytes) -> _Bs:
        """Parse structure from ``bytes``."""
        fields = cls.__bytestruct_fields__
        size = cls.__bytestruct_size__

        if len(b) != size:
            raise ValueError(f'Structure is {size} bytes long, got {len(b)} bytes')

        unpacked_values = struct.unpack(cls.__bytestruct_format__, b)
        values: list[Any] = []
        padding_count = 0

        # Create list of values for dataclass
        # This includes None values for padding and embedded ByteStructs.
        for index, descriptor in enumerate(fields.values()):
            type_ = descriptor.type_origin
            if type_ is NoneType:
                values.append(None)
                padding_count += 1
                continue

            value = unpacked_values[index - padding_count]
            if descriptor.is_bytestruct:
                value = type_.from_bytes(value)
            values.append(value)

        self = cls(*values)

        # Keep packed version of ByteStruct in memory
        # Avoid __setattr__() here because this is a frozen dataclass.
        self.__dict__['__bytestruct_cached__'] = b
        return self

    def __bytes__(self) -> bytes:
        return self.__bytestruct_cached__

    def __len__(self) -> int:
        return self.__bytestruct_size__
