"""pipeline — a data pipeline for ingesting, transforming, and writing CSV datasets."""

from pipeline.errors import (
    IngestError,
    OutputError,
    PipelineError,
    SchemaError,
    TransformError,
    ValidationError,
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
from pipeline.validation import (
    ValidationIssue,
    Validator,
    in_range,
    one_of,
    require_column,
    type_is,
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
    "ValidationError",
    "ValidationIssue",
    "Validator",
    "apply_transforms",
    "coerce_value",
    "compose",
    "in_range",
    "infer_schema",
    "iter_rows",
    "iter_write_csv",
    "one_of",
    "read_csv",
    "require_column",
    "stream_transforms",
    "type_is",
    "write_csv",
]
