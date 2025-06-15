import datetime as dt

import pandera as pa
import pytest
import sqlalchemy
from pandera_alchemy import unified_types as ut


def test_get_unified_type_boolean():
    assert ut.get_unified_type(bool) == ut.Boolean
    assert ut.get_unified_type(pa.Bool) == ut.Boolean
    assert ut.get_unified_type(pa.Bool()) == ut.Boolean
    assert ut.get_unified_type(sqlalchemy.Boolean) == ut.Boolean


def test_get_unified_type_datetime():
    assert ut.get_unified_type(dt.datetime) == ut.DateTime
    assert ut.get_unified_type(pa.DateTime) == ut.DateTime
    assert ut.get_unified_type(pa.DateTime()) == ut.DateTime
    assert ut.get_unified_type(sqlalchemy.DateTime) == ut.DateTime


def test_get_unified_type_float():
    assert ut.get_unified_type(float) == ut.Float
    assert ut.get_unified_type(pa.Float) == ut.Float
    assert ut.get_unified_type(pa.Float()) == ut.Float
    assert ut.get_unified_type(sqlalchemy.Float) == ut.Float


def test_get_unified_type_integer():
    assert ut.get_unified_type(int) == ut.Integer
    assert ut.get_unified_type(pa.Int) == ut.Integer
    assert ut.get_unified_type(pa.Int()) == ut.Integer
    assert ut.get_unified_type(sqlalchemy.Integer) == ut.Integer


def test_get_unified_type_string():
    assert ut.get_unified_type(str) == ut.String
    assert ut.get_unified_type(pa.String) == ut.String
    assert ut.get_unified_type(pa.String()) == ut.String
    assert ut.get_unified_type(sqlalchemy.String) == ut.String


def test_get_unified_type_timedelta():
    assert ut.get_unified_type(dt.timedelta) == ut.Timedelta
    assert ut.get_unified_type(pa.Timedelta) == ut.Timedelta
    assert ut.get_unified_type(pa.Timedelta()) == ut.Timedelta
    assert ut.get_unified_type(sqlalchemy.Interval) == ut.Timedelta


def test_get_unified_type_date():
    assert ut.get_unified_type(dt.date) == ut.Date
    assert ut.get_unified_type(pa.Date) == ut.Date
    assert ut.get_unified_type(pa.Date()) == ut.Date
    assert ut.get_unified_type(sqlalchemy.Date) == ut.Date


def test_get_unified_type_none():
    assert ut.get_unified_type(None) == ut.NoneType
    assert ut.get_unified_type(sqlalchemy.types.NullType) == ut.NoneType


def test_get_unified_type_invalid():
    with pytest.raises(TypeError):
        ut.get_unified_type(sqlalchemy.engine.base.Engine)
    with pytest.raises(TypeError):
        ut.get_unified_type(pytest)
