"""Exception hierarchy for the pipeline package.

Every failure raised by the pipeline derives from :class:`PipelineError`,
so callers can catch a single base type to handle any pipeline failure, or
narrow to a specific subtype for finer-grained handling.
"""

from __future__ import annotations


class PipelineError(Exception):
    """Base class for all pipeline failures."""


class IngestError(PipelineError):
    """Raised when a file cannot be read or a CSV file is malformed."""


class SchemaError(PipelineError):
    """Raised when schema inference fails or a value cannot be coerced."""


class TransformError(PipelineError):
    """Raised when a transform-stage operation fails."""


class OutputError(PipelineError):
    """Raised when the output stage cannot write the transformed records
    (e.g. non-writable path, undecodable output)."""


class ValidationError(PipelineError):
    """Raised for malformed rules or unexpected internal validation failures.

    Validation *issues* (a missing column, a wrong type, an out-of-range
    value, ...) are returned as data via :class:`~pipeline.validation.ValidationIssue`
    objects, never raised. This exception is reserved for programmer errors in
    the validation stage itself: a rule factory given a bad argument, or an
    unexpected internal failure while running the validator.
    """
