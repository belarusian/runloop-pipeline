"""Pipeline orchestration for the pipeline package.

A :class:`Pipeline` composes the ingest and transform stages into a single
callable object. It owns a CSV *source* (a file path) plus an ordered list of
:class:`~pipeline.transform.Transform` ops, and exposes three entry points:

- :meth:`Pipeline.run` — batch: read the whole CSV, apply the transforms in
  order, and return the final list of records.
- :meth:`Pipeline.stream` — streaming: lazily pipe
  :func:`~pipeline.ingest.iter_rows` through
  :func:`~pipeline.transform.stream_transforms`, yielding one record at a time
  so the source is never fully materialized.
- :meth:`Pipeline.schema` — return the inferred
  :class:`~pipeline.schema.Schema` for the source.

The constructor takes a *defensive copy* of the transforms list, so mutating
the caller's list after construction does not affect the pipeline.

Failure contract: this module never raises a bare ``Exception``. Ingest
problems surface as :class:`~pipeline.errors.IngestError` (from
:func:`read_csv` / :func:`iter_rows`); transform problems surface as
:class:`~pipeline.errors.TransformError` (from
:func:`apply_transforms` / :func:`stream_transforms`).
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence

from pipeline.ingest import iter_rows, read_csv
from pipeline.schema import Schema
from pipeline.transform import Transform, apply_transforms, stream_transforms


class Pipeline:
    """A composable CSV pipeline: ingest a source, apply transforms.

    Args:
        source: path to the CSV file to read.
        transforms: the transforms to apply, in order. Stored as a defensive
            copy; mutating the passed list afterwards does not affect this
            pipeline.
        encoding: text encoding used to decode the source. Defaults to
            ``"utf-8-sig"``, which strips a leading UTF-8 BOM and is a no-op
            for plain UTF-8.
        sample_size: bound on how many leading data rows feed schema inference
            (and, for :meth:`stream`, the bounded sample). Coercion is applied
            to every row regardless of this bound.

    Example:
        >>> pipeline = Pipeline("data.csv", [Filter(lambda r: r["id"] > 0)])
        >>> records = pipeline.run()          # batch
        >>> for record in pipeline.stream():  # streaming, lazy
        ...     pass
        >>> schema = pipeline.schema()        # inferred Schema
    """

    def __init__(
        self,
        source: str,
        transforms: Sequence[Transform] = (),
        *,
        encoding: str = "utf-8-sig",
        sample_size: int = 1000,
    ) -> None:
        self._source = source
        self._encoding = encoding
        self._sample_size = sample_size
        # Defensive copy: the caller's list is never aliased.
        self._transforms = list(transforms)

    @property
    def source(self) -> str:
        """The CSV source path."""
        return self._source

    @property
    def encoding(self) -> str:
        """The text encoding used to decode the source."""
        return self._encoding

    @property
    def sample_size(self) -> int:
        """The schema-inference sample bound."""
        return self._sample_size

    @property
    def transforms(self) -> list[Transform]:
        """The transforms, in order (a copy, safe to mutate by the caller)."""
        return list(self._transforms)

    def run(self) -> list[dict[str, int | float | str]]:
        """Run the pipeline in batch mode and return the final records.

        Reads the whole source via :func:`read_csv`, then applies the
        transforms in order via :func:`apply_transforms`.

        Returns:
            The transformed records as a new list.

        Raises:
            IngestError: if the source cannot be read or is malformed.
            SchemaError: if a cell cannot be coerced to its inferred type.
            TransformError: if a transform fails.
        """
        _, records = read_csv(self._source, self._encoding, self._sample_size)
        return apply_transforms(records, self._transforms)

    def stream(self) -> Iterator[dict[str, int | float | str]]:
        """Run the pipeline in streaming mode, lazily yielding records.

        Pipes :func:`iter_rows` through :func:`stream_transforms`. The source
        is never fully materialized: one record is pulled, transformed, and
        yielded at a time.

        Yields:
            One transformed record per surviving input record.

        Raises:
            IngestError: if the source cannot be read or is malformed.
            SchemaError: if a cell cannot be coerced to its inferred type.
            TransformError: if any transform is not streamable (e.g. a
                batch-only :class:`~pipeline.transform.Aggregate`) or fails.
        """
        return stream_transforms(
            iter_rows(self._source, self._encoding, self._sample_size),
            self._transforms,
        )

    def schema(self) -> Schema:
        """Return the inferred :class:`Schema` for the source.

        Reads the source via :func:`read_csv` and returns the schema half of
        the returned tuple.

        Returns:
            The inferred :class:`Schema`.

        Raises:
            IngestError: if the source cannot be read or is malformed.
            SchemaError: if schema inference fails.
        """
        schema, _ = read_csv(self._source, self._encoding, self._sample_size)
        return schema
