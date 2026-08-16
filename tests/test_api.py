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
        # Transformation phase
        "Transform",
        "Filter",
        "MapColumn",
        "Rename",
        "Select",
        "Aggregate",
        "apply_transforms",
        # Streaming + composition (Cycle 4)
        "stream_transforms",
        "compose",
        "Composed",
        # Orchestration (Cycle 5)
        "Pipeline",
    }
    assert expected.issubset(set(pipeline.__all__))


def test_version_present():
    assert hasattr(pipeline, "__version__")
