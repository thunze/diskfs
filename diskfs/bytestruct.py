"""Packing and validation of binary data."""

from __future__ import annotations

import struct
from dataclasses import InitVar
from typing import Any, ClassVar, Literal, NamedTuple, TypeVar

from typing_extensions import Annotated, get_args, get_origin, get_type_hints

from .base import ValidationError
from .typing_ import NoneType

__all__ = ["ByteStruct"]


INT_CONVERSION = {1: "B", 2: "H", 4: "I", 8: "Q"}
FLOAT_CONVERSION = {2: "e", 4: "f", 8: "d"}
SIGNED_SPECIFIERS = ("signed", "unsigned")
INTERNAL_NAMES = (
    "__bytestruct_fields__",
    "__bytestruct_format__",
    "__bytestruct_size__",
    "__bytestruct_cached__",
)

_Bs = TypeVar("_Bs", bound="ByteStruct")


class _FieldDescriptor(NamedTuple):
    """Metadata about a field of a `ByteStruct`.

    - `type_origin`: Origin of an `Annotated` type (e.g. `bytes` for
        `Annotated[bytes, 4]`) or the according `ByteStruct` subclass if the
        field represents an embedded `ByteStruct`.
    - `type_args`: Tuple of the metadata added to `Annotated` (e.g. `(4,)` for
        `Annotated[bytes, 4]`) if the field has an `Annotated` type, () otherwise.
    - `is_bytestruct`: True if the field represents an embedded `ByteStruct`.
        Saved separately to avoid more costly calls of `instanceof(type_origin)`.
    """

    type_origin: Any
    type_args: tuple[Any, ...] = ()
    is_bytestruct: bool = False


class _ByteStructMeta(type):
    """Metaclass of `ByteStruct`.

    Analyzes the type annotations found in a `ByteStruct` subclass and sets
    `__bytestruct_fields__`, `__bytestruct_format__` and `__bytestruct_size__`
    accordingly.

    - `__bytestruct_fields__` is a mapping of field names (`str`) to field
        descriptors (`_FieldDescriptor`). A field descriptor contains metadata about
        a field of the `ByteStruct`. See `_FieldDescriptor` for more information.
    - `__bytestruct_format__` is the format string which is passed to
        `struct.pack()` and `struct.unpack()` to convert between the values of the
        `ByteStruct` and its `bytes` form as read from or written to a disk.
    - `__bytestruct_size__` is the size of the `bytes` form of the `ByteStruct`
        in bytes.
    """

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> _ByteStructMeta:
        """Provide a signature of `__new__()` which allows specifying `kwargs`
        like `byteorder` when subclassing `ByteStruct`.
        """
        return super().__new__(mcs, name, bases, namespace)

    def __init__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        byteorder: Literal["<", ">", "!", "="] = "<",
    ):
        super().__init__(name, bases, namespace)
        if not bases:
            return  # cls is ByteStruct

        type_hints = get_type_hints(cls, include_extras=True)
        format_ = f"{byteorder}"
        fields = {}

        for name, type_ in type_hints.items():
            if name in INTERNAL_NAMES or type(type_) is InitVar:
                continue

            origin = get_origin(type_)
            if origin is ClassVar:
                continue

            # Embedded ByteStruct, treat as bytes
            if isinstance(type_, cls.__class__) and type_ is not ByteStruct:
                format_ += f"{len(type_)}s"
                fields[name] = _FieldDescriptor(type_, is_bytestruct=True)
                continue

            if origin is not Annotated:
                raise TypeError(
                    f"Unannotated type {type_} of field {name!r} is not allowed for "
                    f"ByteStruct"
                )

            # Annotated type
            args = get_args(type_)
            annotated_type = args[0]
            size = args[1]
            if not isinstance(size, int):
                raise TypeError("Field size must be specified as int")
            if size < 1:
                raise ValueError("Field size must be greater than or equal to 1")

            if annotated_type is int:
                signed = False
                if len(args) > 2:
                    if args[2] not in SIGNED_SPECIFIERS:
                        raise ValueError(
                            f"Invalid specifier {args[2]} on field {name!r}, must be "
                            f"one of {SIGNED_SPECIFIERS}"
                        )
                    signed = args[2] == "signed"
                if size not in INT_CONVERSION.keys():
                    raise ValueError(
                        f"Invalid int field size {size}, must be one of "
                        f"{tuple(INT_CONVERSION.keys())}"
                    )
                format_specifier = INT_CONVERSION[size]
                if signed:
                    format_specifier = format_specifier.lower()
                format_ += format_specifier

            elif annotated_type is float:
                if size not in FLOAT_CONVERSION.keys():
                    raise ValueError(
                        f"Invalid float field size {size}, must be one of "
                        f"{tuple(FLOAT_CONVERSION.keys())}"
                    )
                format_ += FLOAT_CONVERSION[size]

            elif annotated_type is bytes:
                format_ += f"{size}s"
            elif annotated_type is NoneType:
                format_ += f"{size}x"  # pad bytes
            else:
                raise TypeError(
                    f"Annotated type {args[0]} of field {name!r} is not allowed for "
                    f"ByteStruct"
                )

            fields[name] = _FieldDescriptor(annotated_type, args[1:])

        cls.__bytestruct_fields__ = fields
        cls.__bytestruct_format__ = format_
        cls.__bytestruct_size__ = struct.calcsize(format_)

    def __len__(cls) -> int:
        """Size of the `bytes` form of the `ByteStruct` in bytes."""
        return cls.__bytestruct_size__


class ByteStruct(metaclass=_ByteStructMeta):
    """Packed binary data.

    Basically a user-friendy wrapper of a struct according to the `struct` module
    with a few extra features which include:

    - Embedding of structs in other structs
    - Access to values by field name through integration with the `dataclass`
        decorator
    - User-friendly definition of field types
    - Validation of values including an easy way to add custom validation logic

    Note that every `ByteStruct` subclass must be a frozen `dataclass`.

    Example::

        @dataclasses.dataclass(frozen=True)
        class MyStruct(ByteStruct, byteorder='<'):

            field_1: Annotated[int, 2]            # unsigned int of size 2 bytes
            field_2: Annotated[int, 4]            # unsigned int of size 4 bytes
            field_3: Annotated[int, 4, 'signed']  # signed int of size 4 bytes

            field_4: Annotated[float, 4]          # float of size 4 bytes
            field_5: Annotated[bytes, 4]          # bytes of size 4
            field_6: Annotated[None, 8]           # 8 pad bytes

            field_7: MySecondStruct               # embedded ByteStruct

    The `byteorder` argument can be one of `('<', '>', '!', '=')`.
    Supported field types / formats in terms of the `struct` module are:

    - b, B, h, H, i, I, q, Q, e, f, d, s, x

    See the documentation of the `struct` module for more information:

        https://docs.python.org/3/library/struct.html

    Note that field values are not validated against their type annotations at
    runtime except for the metadata carried along with them through arguments of
    `typing.Annotated`.

    Custom validation logic can be added by overriding the `validate()` method.
    """

    # Populated per class
    __bytestruct_fields__: "dict[str, _FieldDescriptor]"
    __bytestruct_format__: str
    __bytestruct_size__: int

    # Populated per instance
    __bytestruct_cached__: bytes

    @classmethod
    def _check_direct_instantiation(cls) -> None:
        """Raise `TypeError` if it is tried to directly instantiate `ByteStruct`
        and not a subclass of `ByteStruct`.
        """
        if cls.__bases__ == (object,):
            raise TypeError(f"Cannot directly instantiate {cls.__name__}")

    @classmethod
    def _check_frozen_dataclass(cls) -> None:
        """Raise `TypeError` if it is tried to instantiate a subclass of
        `ByteStruct` which is not a frozen `dataclass`.
        """
        params: Any = getattr(cls, "__dataclass_params__", None)
        if params is None or not params.frozen:
            raise TypeError("ByteStruct subclass must be a frozen dataclass")

    # noinspection PyUnusedLocal
    def __init__(self, *args: Any, **kwargs: Any):
        self._check_direct_instantiation()
        self._check_frozen_dataclass()

    def __post_init__(self) -> None:
        """Executed after instance creation as we expect every instance to be a
        `dataclass`.

        Triggers the internal and the user-defined validation logic.
        """
        self._check_frozen_dataclass()
        if not hasattr(self, "__bytestruct_cached__"):
            self._validate_and_cache()
        self.validate()

    def _validate_and_cache(self) -> None:
        """Validate field values against the defined formats.

        Because this involves creating a `bytes` version of the `ByteStruct`
        instance anyway, we cache the resulting `bytes` object.
        """
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
                        f"Value of field {name!r} must be of length {size} bytes, "
                        f"got {len(value)} bytes"
                    )

            values.append(value)

        # All other values (int and float) are validated via struct.pack().
        try:
            bytes_ = struct.pack(self.__bytestruct_format__, *values)
        except (struct.error, OverflowError) as e:
            raise ValidationError(
                f"Value out of range (format is {self.__bytestruct_format__!r})"
            ) from e

        # Keep packed version of ByteStruct in memory
        # Avoid __setattr__() here because this is a frozen dataclass.
        self.__dict__["__bytestruct_cached__"] = bytes_

    def validate(self) -> None:
        """Custom validation logic.

        Automatically executed after object creation, but after validation of the
        field values against their corresponding formats.
        """

    @classmethod
    def from_bytes(cls: type[_Bs], b: bytes) -> _Bs:
        """Parse structure from `bytes`."""
        cls._check_direct_instantiation()

        fields = cls.__bytestruct_fields__
        size = cls.__bytestruct_size__

        if len(b) != size:
            raise ValueError(f"Structure is {size} bytes long, got {len(b)} bytes")

        unpacked_values = struct.unpack(cls.__bytestruct_format__, b)
        values: list[Any] = []
        padding_count = 0

        # Create list of values for dataclass
        # This includes embedded ByteStructs and None values for padding.
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
        self.__dict__["__bytestruct_cached__"] = b
        return self

    def __bytes__(self) -> bytes:
        """`bytes` form of the `ByteStruct` instance."""
        return self.__bytestruct_cached__

    def __len__(self) -> int:
        """Size of the `bytes` form of the `ByteStruct` in bytes."""
        return self.__bytestruct_size__
