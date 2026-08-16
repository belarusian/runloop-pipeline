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


def test_schema_to_dict():
    schema = Schema(columns=(Column("a", int), Column("b", float), Column("c", str)))
    assert schema.to_dict() == {"a": int, "b": float, "c": str}
    # to_dict is a plain dict and matches types()
    assert isinstance(schema.to_dict(), dict)
    assert schema.to_dict() == schema.types()


def test_schema_len():
    schema = Schema(columns=(Column("a", int), Column("b", str)))
    assert len(schema) == 2
    assert len(Schema(columns=())) == 0


def test_schema_iter():
    schema = Schema(columns=(Column("a", int), Column("b", str), Column("c", float)))
    columns = list(schema)
    assert columns == [Column("a", int), Column("b", str), Column("c", float)]
    # iteration yields Column objects in order
    assert [c.name for c in schema] == ["a", "b", "c"]


def test_schema_project_valid():
    schema = Schema(columns=(Column("a", int), Column("b", str), Column("c", float)))
    projected = schema.project(["c", "a"])
    assert projected.names() == ["c", "a"]
    assert projected.types() == {"c": float, "a": int}
    assert len(projected) == 2
    # original is unchanged (frozen, new object)
    assert schema.names() == ["a", "b", "c"]
    assert projected is not schema


def test_schema_project_single_column():
    schema = Schema(columns=(Column("a", int), Column("b", str)))
    projected = schema.project(["b"])
    assert projected.names() == ["b"]
    assert projected.types() == {"b": str}


def test_schema_project_unknown_name_raises():
    schema = Schema(columns=(Column("a", int), Column("b", str)))
    with pytest.raises(SchemaError):
        schema.project(["a", "nope"])


def test_infer_schema_sample_size_bounded():
    # First 2 rows are numeric; later rows are strings. With sample_size=2 the
    # column is inferred as int, proving only the leading rows were inspected.
    rows = [["1"], ["2"], ["x"], ["y"]]
    full = infer_schema(rows)
    assert full.types() == {"col_0": str}
    bounded = infer_schema(rows, sample_size=2)
    assert bounded.types() == {"col_0": int}


def test_infer_schema_sample_size_none_preserves_behavior():
    rows = [["1"], ["2"], ["x"]]
    assert infer_schema(rows).types() == infer_schema(rows, sample_size=None).types()
    assert infer_schema(rows, sample_size=None).types() == {"col_0": str}


def test_infer_schema_sample_size_larger_than_rows():
    rows = [["1"], ["2"]]
    assert infer_schema(rows, sample_size=100).types() == {"col_0": int}
