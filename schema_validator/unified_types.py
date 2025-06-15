import datetime as dt
from dataclasses import dataclass
from typing import Optional, Type, Union

import pandera as pa
import sqlalchemy


@dataclass
class UnifiedType:
    """A unified type that maps types in Pandera and SQLAlchemy for easy comparison.

    To avoid having to support every possible type in Pandera and SQLAlchemy,
    we define a set of unified types from which all other types are derived.

    SQLAlchemy Types Documentation: https://docs.sqlalchemy.org/en/20/core/type_basics.html
    Pandera Types Documentation: https://pandera.readthedocs.io/en/stable/dtypes.html

    For example, we can't support database-specific types like Redshift's `TIMESTAMPTZ` or `VARCHAR(256)`:
    instead, these get treated as the more general `DateTime` and `String` types.
    """

    native_type: Optional[type]
    pandera_dtype: Optional[Type[pa.DataType]]
    sqlalchemy_dtype: Optional[sqlalchemy.types.TypeEngine]

    def __repr__(self):
        return self.__class__.__name__


@dataclass
class Boolean(UnifiedType):
    native_type = bool
    pandera_dtype = pa.Bool
    sqlalchemy_dtype = sqlalchemy.Boolean


@dataclass
class DateTime(UnifiedType):
    native_type = dt.datetime

    pandera_dtype = pa.DateTime

    # Base class for sqlalchemy.TIMESTAMP, sqlalchemy.redshift.TIMESTAMPTZ
    sqlalchemy_dtype = sqlalchemy.DateTime


@dataclass
class Float(UnifiedType):
    native_type = float

    pandera_dtype = pa.Float

    # Base class for sqlalchemy.Float, sqlalchemy.Double
    sqlalchemy_dtype = sqlalchemy.Float


@dataclass
class Integer(UnifiedType):
    native_type = int

    pandera_dtype = pa.Int

    # Base class for sqlalchemy.SmallInteger, sqlalchemy.BigInteger
    sqlalchemy_dtype = sqlalchemy.Integer


@dataclass
class String(UnifiedType):
    native_type = str

    pandera_dtype = pa.String

    # Base class for sqlalchemy.Text, sqlalchemy.Unicode, sqlalchemy.UnicodeText
    sqlalchemy_dtype = sqlalchemy.String


@dataclass
class Timedelta(UnifiedType):
    native_type = dt.timedelta

    pandera_dtype = pa.Timedelta

    sqlalchemy_dtype = sqlalchemy.Interval


@dataclass
class Date(UnifiedType):
    native_type = dt.date

    pandera_dtype = pa.Date

    sqlalchemy_dtype = sqlalchemy.Date


@dataclass
class NoneType(UnifiedType):
    native_type = None
    pandera_dtype = None
    sqlalchemy_dtype = sqlalchemy.types.NullType


# Alias for types that can be converted to a UnifiedType
SUPPORTED_TYPES = Union[
    type, None, pa.DataType, Type[pa.DataType], sqlalchemy.types.TypeEngine, Type[sqlalchemy.types.TypeEngine]
]


def get_unified_type(dtype: SUPPORTED_TYPES) -> Type[UnifiedType]:
    """Convert an object or class representing a Pandera or SQLAlchemy type to a UnifiedType."""
    if dtype in (NoneType.native_type, NoneType.pandera_dtype, NoneType.sqlalchemy_dtype):
        return NoneType

    UNIFIED_TYPES = [String, Boolean, DateTime, Float, Integer, Timedelta, Date]
    for unified_type in UNIFIED_TYPES:
        pandera_match = type_match(dtype, unified_type)
        sql_match = type_match(dtype, unified_type)
        native_match = type_match(dtype, unified_type)
        if pandera_match or sql_match or native_match:
            return unified_type
    raise TypeError(f"Type {dtype} not supported.")


def type_match(typ: SUPPORTED_TYPES, reference: Type[UnifiedType]) -> bool:
    """A helper function to check if a given type matches a unified type.

    This is flexible enough to handle passing a Pandera type, or SQLAlchemy type, or their classes.
    """
    if typ is None:
        raise ValueError("Can't call type_match with None type.")

    # Determine if we're comparing types or classes
    type_comparator = issubclass if isinstance(typ, type) else isinstance

    if type_comparator(typ, pa.DataType):
        reference_type = reference.pandera_dtype
    elif type_comparator(typ, sqlalchemy.types.TypeEngine):
        reference_type = reference.sqlalchemy_dtype
    else:
        # This might be a shaky assumption
        reference_type = reference.native_type

    if reference_type is None:
        raise ValueError("Got a None for the reference type in type_match")

    return type_comparator(typ, reference_type)


class UnifiedColumn:
    """A representation of a column that can be used to compare Pandera and SQLAlchemy columns."""

    def __init__(self, name: str, dtype: Type[UnifiedType], nullable: Optional[bool]):
        self.name = name
        self.dtype = dtype
        self.nullable = nullable

    def __repr__(self):
        return self.name


if __name__ == "__main__":
    # Test the UnifiedType class
    assert get_unified_type(str) == String
    assert get_unified_type(pa.String) == String
    assert get_unified_type(pa.String()) == String
    assert get_unified_type(sqlalchemy.String) == String

    import numpy as np

    assert get_unified_type(np.datetime64) == DateTime
