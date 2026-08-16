"""Tests for the pipeline exception hierarchy."""

import pytest

from pipeline.errors import (
    IngestError,
    OutputError,
    PipelineError,
    SchemaError,
    TransformError,
    ValidationError,
)


@pytest.mark.parametrize(
    "exc_type",
    [IngestError, SchemaError, TransformError, OutputError, ValidationError],
)
def test_subclasses_are_pipeline_errors(exc_type):
    assert issubclass(exc_type, PipelineError)


@pytest.mark.parametrize(
    "exc_type",
    [PipelineError, IngestError, SchemaError, TransformError, OutputError, ValidationError],
)
def test_each_error_is_raisable(exc_type):
    with pytest.raises(exc_type):
        raise exc_type("boom")


def test_pipeline_error_is_exception():
    assert issubclass(PipelineError, Exception)


def test_catch_by_base_type():
    with pytest.raises(PipelineError):
        raise IngestError("bad file")


def test_output_error_catchable_as_pipeline_error():
    with pytest.raises(PipelineError):
        raise OutputError("cannot write output")


def test_output_error_is_distinct_from_ingest_error():
    # OutputError must not be an IngestError (write failures are not read
    # failures) and vice versa.
    assert not issubclass(OutputError, IngestError)
    assert not issubclass(IngestError, OutputError)
