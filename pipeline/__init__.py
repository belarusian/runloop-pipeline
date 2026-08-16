"""pipeline — a data pipeline for ingesting and transforming CSV datasets."""

from pipeline.errors import IngestError, PipelineError, SchemaError, TransformError
from pipeline.ingest import iter_rows, read_csv
from pipeline.schema import Column, Schema, coerce_value, infer_schema
from pipeline.transform import (
    Aggregate,
    Filter,
    MapColumn,
    Rename,
    Select,
    Transform,
    apply_transforms,
)

__version__ = "0.1.0"

__all__ = [
    "Aggregate",
    "Column",
    "Filter",
    "IngestError",
    "MapColumn",
    "PipelineError",
    "Rename",
    "Schema",
    "SchemaError",
    "Select",
    "Transform",
    "TransformError",
    "apply_transforms",
    "coerce_value",
    "infer_schema",
    "iter_rows",
    "read_csv",
]
