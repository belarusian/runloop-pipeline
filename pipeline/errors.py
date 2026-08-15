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
