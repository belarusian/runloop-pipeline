"""Tests for the pipeline schema model and inference."""

import pytest

from pipeline.errors import SchemaError
from pipeline.schema import Column, Schema, coerce_value, infer_schema


def test_infer_schema_int():
    schema = infer_schema([["1", "2"], ["3", "4"]])
    assert schema.types() == {"col_0": int, "col_1": int}


def test_infer_schema_float():
    schema = infer_schema([["1.5", "2.0"], ["3.25", "4"]])
    assert schema.types() == {"col_0": float, "col_1": float}


def test_infer_schema_str():
    schema = infer_schema([["a", "b"], ["c", "d"]])
    assert schema.types() == {"col_0": str, "col_1": str}


def test_infer_schema_mixed_is_str():
    schema = infer_schema([["1", "x"], ["2", "y"]])
    assert schema.types() == {"col_0": int, "col_1": str}


def test_infer_schema_empty_column_is_str():
    schema = infer_schema([["1", ""], ["2", ""]])
    assert schema.types() == {"col_0": int, "col_1": str}


def test_infer_schema_empty_rows_raises():
    with pytest.raises(SchemaError):
        infer_schema([])


def test_infer_schema_uses_header_when_given():
    schema = infer_schema([["1"]], header=["count"])
    assert schema.names() == ["count"]
    assert schema.types() == {"count": int}


def test_coerce_value_int():
    assert coerce_value("42", int) == 42


def test_coerce_value_float():
    assert coerce_value("3.14", float) == 3.14


def test_coerce_value_str():
    assert coerce_value("hello", str) == "hello"


def test_coerce_value_int_failure():
    with pytest.raises(SchemaError):
        coerce_value("not-a-number", int)


def test_coerce_value_float_failure():
    with pytest.raises(SchemaError):
        coerce_value("not-a-number", float)


def test_coerce_value_unsupported_type():
    with pytest.raises(SchemaError):
        coerce_value("1", bool)


def test_schema_names():
    schema = Schema(columns=(Column("a", int), Column("b", str)))
    assert schema.names() == ["a", "b"]


def test_schema_column_lookup():
    schema = Schema(columns=(Column("a", int), Column("b", str)))
    assert schema.column("b").type is str


def test_schema_column_missing_raises():
    schema = Schema(columns=(Column("a", int),))
    with pytest.raises(SchemaError):
        schema.column("nope")


def test_schema_types():
    schema = Schema(columns=(Column("a", int), Column("b", float)))
    assert schema.types() == {"a": int, "b": float}
