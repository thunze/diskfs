"""Byte struct packing and validation."""

import struct
from abc import ABCMeta
from types import NoneType
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Literal,
    Type,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)

from .base import ValidationError

if TYPE_CHECKING:
    from .volume import Volume

__all__ = ['ByteStruct', 'BYTE_ORDERS']


BYTE_ORDERS = ('<', '>', '!', '=')
SIGNED_SPECIFIERS = ('signed', 'unsigned')
INT_CONVERSION = {1: 'B', 2: 'H', 4: 'I', 8: 'Q'}
FLOAT_CONVERSION = {2: 'e', 4: 'f', 8: 'd'}


def _is_bytestruct_type(type_: Any) -> bool:
    """Return whether ``type_`` is a class which inherits from ``ByteStruct``."""
    return isinstance(type_, type) and issubclass(type_, ByteStruct)


class _ByteStructMeta(ABCMeta):
    """Metaclass for ``ByteStruct``."""

    def __init__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        byteorder: Literal['<', '>', '!', '='] = '<',
    ):
        super().__init__(name, bases, namespace)
        type_hints = get_type_hints(cls, include_extras=True)
        format_ = f'{byteorder}'

        for name, type_ in type_hints.items():
            if get_origin(type_) is ClassVar:
                continue

            if get_origin(type_) is Annotated:
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
                                f'Invalid specifier {args[2]} on field {name}, must '
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
                        f'Annotated type {args[0]} of field {name} is not allowed '
                        f'for ByteStruct'
                    )

            elif _is_bytestruct_type(type_):
                # embedded ByteStruct, treat as bytes
                size = type_.__bytestruct_size__
                format_ += f'{size}s'

            else:
                raise TypeError(
                    f'Type {type_} of field {name} is not allowed for ByteStruct'
                )

        # exclude classvars
        fields = {k: v for k, v in type_hints.items() if get_origin(v) is not ClassVar}
        cls.__bytestruct_fields__ = fields
        cls.__bytestruct_format__ = format_
        cls.__bytestruct_size__ = struct.calcsize(format_)

    def __len__(cls) -> int:
        return cls.__bytestruct_size__


_BS = TypeVar('_BS', bound='ByteStruct')


class ByteStruct(metaclass=_ByteStructMeta):
    """Byte structure."""

    # We exclude these dunder attributes from dataclass via ClassVar, but they will
    # still be accessible from the instance.
    __bytestruct_fields__: ClassVar[dict[str, Any]]
    __bytestruct_format__: ClassVar[str]
    __bytestruct_size__: ClassVar[int]

    def validate_against_annotations(self) -> None:
        """Validate against field annotations.

        Automatically executed after object creation.
        """
        for name, type_ in self.__bytestruct_fields__.items():
            value = getattr(self, name)

            if get_origin(type_) is ClassVar:
                continue

            if get_origin(type_) is Annotated:
                # Annotated
                args = get_args(type_)
                annotated_type = args[0]
                size = args[1]

                # noinspection PyTypeHints
                if not isinstance(value, annotated_type):
                    raise TypeError(
                        f'Value {value!r} of field {name} is not of specified type '
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
                            f'Value {value} of field {name} must be in range '
                            f'({min_}, {max_})'
                        )

                elif annotated_type is bytes:
                    if len(value) != size:
                        raise ValidationError(
                            f'Field value must be of length {size} bytes, got '
                            f'{len(value)} bytes'
                        )

            elif _is_bytestruct_type(type_):
                # embedded ByteStruct
                if not isinstance(value, type_):
                    raise TypeError(
                        f'Value {value!r} of field {name} is not of specified type '
                        f'{type_}'
                    )

    def validate(self) -> None:
        """Custom validation.

        Automatically executed after object creation, but after field annotation
        validation.
        """

    def validate_for_volume(self, volume: 'Volume', *, recurse: bool = False) -> None:
        """Custom validation to check suitability for a specific volume.

        If ``recurse`` is set, ``validate_for_volume`` is also called on every
        embedded ``ByteStruct`` with ``recurse`` set.
        """
        if recurse:
            for name, type_ in self.__bytestruct_fields__.items():
                value = getattr(self, name)
                if _is_bytestruct_type(type_):
                    value.validate_for_volume(volume, recurse=True)

    # noinspection PyUnusedLocal
    def __init__(self, *args: Any, **kwargs: Any):
        # dataclass replaces __init__, so call __post_init__ if we're not replaced.
        self.__post_init__()

    def __post_init__(self) -> None:
        self.validate_against_annotations()  # TODO
        self.validate()

    @classmethod
    def from_bytes(cls: Type[_BS], b: bytes) -> _BS:
        """Parse structure from ``bytes``."""
        fields = cls.__bytestruct_fields__
        size = cls.__bytestruct_size__

        if len(b) != size:
            raise ValueError(f'Structure must be {size} bytes long, got {len(b)} bytes')

        unpacked_values = struct.unpack(cls.__bytestruct_format__, b)
        values = []

        # create embedded bytestructs
        for type_, unpacked_value in zip(fields.values(), unpacked_values):
            if _is_bytestruct_type(type_):
                values.append(type_.from_bytes(unpacked_value))
            else:
                values.append(unpacked_value)

        return cls(*values)

    def __bytes__(self) -> bytes:
        names = self.__bytestruct_fields__.keys()
        values = (getattr(self, name) for name in names)

        # convert embedded bytestructs to bytes
        prepared_values = (
            bytes(value) if isinstance(value, ByteStruct) else value for value in values
        )
        return struct.pack(self.__bytestruct_format__, *prepared_values)

    def __len__(self) -> int:
        return self.__bytestruct_size__
