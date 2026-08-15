"""pipeline — a data pipeline for ingesting and transforming CSV datasets."""

from pipeline.errors import IngestError, PipelineError, SchemaError, TransformError
from pipeline.ingest import read_csv
from pipeline.schema import Column, Schema, coerce_value, infer_schema

__version__ = "0.1.0"

__all__ = [
    "Column",
    "IngestError",
    "PipelineError",
    "Schema",
    "SchemaError",
    "TransformError",
    "coerce_value",
    "infer_schema",
    "read_csv",
]
