"""Tests for the pipeline public API exports."""

import pipeline


def test_all_exports_are_present():
    for name in pipeline.__all__:
        assert hasattr(pipeline, name), f"missing export: {name}"


def test_expected_public_api():
    expected = {
        "read_csv",
        "Schema",
        "Column",
        "infer_schema",
        "coerce_value",
        "PipelineError",
        "IngestError",
        "SchemaError",
        "TransformError",
    }
    assert expected.issubset(set(pipeline.__all__))


def test_version_present():
    assert hasattr(pipeline, "__version__")
