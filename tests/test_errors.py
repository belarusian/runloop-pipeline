"""Tests for the pipeline exception hierarchy."""

import pytest

from pipeline.errors import (
    IngestError,
    PipelineError,
    SchemaError,
    TransformError,
)


@pytest.mark.parametrize(
    "exc_type",
    [IngestError, SchemaError, TransformError],
)
def test_subclasses_are_pipeline_errors(exc_type):
    assert issubclass(exc_type, PipelineError)


@pytest.mark.parametrize(
    "exc_type",
    [PipelineError, IngestError, SchemaError, TransformError],
)
def test_each_error_is_raisable(exc_type):
    with pytest.raises(exc_type):
        raise exc_type("boom")


def test_pipeline_error_is_exception():
    assert issubclass(PipelineError, Exception)


def test_catch_by_base_type():
    with pytest.raises(PipelineError):
        raise IngestError("bad file")
