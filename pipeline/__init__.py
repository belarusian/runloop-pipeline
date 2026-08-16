"""pipeline — a data pipeline for ingesting, transforming, and writing CSV datasets."""

from pipeline.errors import (
    IngestError,
    OutputError,
    PipelineError,
    SchemaError,
    TransformError,
)
from pipeline.ingest import iter_rows, read_csv
from pipeline.output import iter_write_csv, write_csv
from pipeline.pipeline import Pipeline
from pipeline.schema import Column, Schema, coerce_value, infer_schema
from pipeline.transform import (
    Aggregate,
    Composed,
    Filter,
    MapColumn,
    Rename,
    Select,
    Transform,
    apply_transforms,
    compose,
    stream_transforms,
)

__version__ = "0.1.0"

__all__ = [
    "Aggregate",
    "Column",
    "Composed",
    "Filter",
    "IngestError",
    "MapColumn",
    "OutputError",
    "Pipeline",
    "PipelineError",
    "Rename",
    "Schema",
    "SchemaError",
    "Select",
    "Transform",
    "TransformError",
    "apply_transforms",
    "coerce_value",
    "compose",
    "infer_schema",
    "iter_rows",
    "iter_write_csv",
    "read_csv",
    "stream_transforms",
    "write_csv",
]
