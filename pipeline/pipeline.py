"""Pipeline orchestration for the pipeline package.

A :class:`Pipeline` composes the ingest, transform, and output stages into a
single callable object. It owns one or more CSV *sources* (file paths) plus an
ordered list of :class:`~pipeline.transform.Transform` ops, and exposes entry
points for both batch and streaming execution:

- :meth:`Pipeline.run` — batch: read every source, apply the transforms in
  order, and return the final list of records.
- :meth:`Pipeline.stream` — streaming: lazily pipe
  :func:`~pipeline.ingest.iter_rows` (over each source, in order) through
  :func:`~pipeline.transform.stream_transforms`, yielding one record at a time
  so no source is ever fully materialized.
- :meth:`Pipeline.schema` — return the inferred
  :class:`~pipeline.schema.Schema` of the first source.
- :meth:`Pipeline.to_csv` — batch output: run the pipeline and write the
  records to a CSV file via :func:`~pipeline.output.write_csv`.
- :meth:`Pipeline.stream_to_csv` — streaming output: pipe :meth:`stream`
  through :func:`~pipeline.output.iter_write_csv` so records are written one at
  a time without full materialization.

Multi-source: the constructor accepts a bare ``str`` path or a
``Sequence[str]`` of paths. A bare ``str`` is normalized to a one-element
list; a sequence is stored as a defensive copy. :meth:`run` concatenates the
sources in order; :meth:`stream` chains them lazily in order; :meth:`schema`
returns the schema of the first source. A single-source pipeline behaves
exactly as before.

The constructor takes a *defensive copy* of the transforms list, so mutating
the caller's list after construction does not affect the pipeline.

Failure contract: this module never raises a bare ``Exception``. Ingest
problems surface as :class:`~pipeline.errors.IngestError` (from
:func:`read_csv` / :func:`iter_rows`); transform problems surface as
:class:`~pipeline.errors.TransformError` (from
:func:`apply_transforms` / :func:`stream_transforms`); output problems surface
as :class:`~pipeline.errors.OutputError` (from
:func:`~pipeline.output.write_csv` / :func:`~pipeline.output.iter_write_csv`).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence

from pipeline.errors import PipelineError
from pipeline.ingest import iter_rows, read_csv
from pipeline.output import iter_write_csv, write_csv
from pipeline.schema import Schema
from pipeline.transform import Transform, apply_transforms, stream_transforms
from pipeline.validation import ValidationIssue, Validator


class Pipeline:
    """A composable CSV pipeline: ingest sources, apply transforms, write output.

    Args:
        source: a single CSV path (``str``) or a sequence of CSV paths
            (``Sequence[str]``). A bare ``str`` is treated as one source; a
            sequence is stored as a defensive copy and must be non-empty.
        transforms: the transforms to apply, in order. Stored as a defensive
            copy; mutating the passed list afterwards does not affect this
            pipeline.
        encoding: text encoding used to decode each source. Defaults to
            ``"utf-8-sig"``, which strips a leading UTF-8 BOM and is a no-op
            for plain UTF-8.
        sample_size: bound on how many leading data rows feed schema inference
            (and, for :meth:`stream`, the bounded sample) per source. Coercion
            is applied to every row regardless of this bound.
        schema: an optional explicit :class:`~pipeline.schema.Schema` to use
            verbatim as the source of truth for ingestion. When supplied, it is
            threaded through :meth:`run`, :meth:`stream`, :meth:`to_csv`, and
            :meth:`stream_to_csv`, pinning column types across the whole
            pipeline, and :meth:`schema` returns it directly. When ``None``
            (the default), the schema is inferred from the data as before.

    Example:
        >>> pipeline = Pipeline("data.csv", [Filter(lambda r: r["id"] > 0)])
        >>> records = pipeline.run()          # batch
        >>> for record in pipeline.stream():  # streaming, lazy
        ...     pass
        >>> schema = pipeline.schema()        # inferred Schema (first source)
        >>> pipeline.to_csv("out.csv")        # batch output
        >>> pipeline.stream_to_csv("out.csv") # streaming output
    """

    def __init__(
        self,
        source: str | Sequence[str],
        transforms: Sequence[Transform] = (),
        *,
        encoding: str = "utf-8-sig",
        sample_size: int = 1000,
        schema: Schema | None = None,
    ) -> None:
        self._sources = self._normalize_sources(source)
        self._encoding = encoding
        self._sample_size = sample_size
        # Explicit schema override (issues #49-#52). When supplied, it is the
        # source of truth for ingestion and is threaded through every stage;
        # when None, the schema is inferred as before.
        self._schema = schema
        # Defensive copy: the caller's list is never aliased.
        self._transforms = list(transforms)

    @staticmethod
    def _normalize_sources(source: str | Sequence[str]) -> list[str]:
        """Normalize *source* into a defensive ``list[str]`` of source paths.

        A bare ``str`` (or any non-sequence scalar such as a ``Path``) becomes
        a one-element list. A non-empty ``Sequence`` is copied defensively.

        Raises:
            PipelineError: if a sequence source is empty.
        """
        if isinstance(source, str):
            return [source]
        if isinstance(source, Sequence):
            sources = list(source)
            if not sources:
                raise PipelineError("source sequence must not be empty")
            return sources
        # A single non-string source (e.g. a pathlib.Path).
        return [source]

    @property
    def source(self) -> str | list[str]:
        """The CSV source path(s).

        Returns the original source when the pipeline has exactly one source
        (the common single-source case, preserving the historical ``str``
        contract), or a defensive copy of the source list when the pipeline
        has multiple sources.
        """
        if len(self._sources) == 1:
            return self._sources[0]
        return list(self._sources)

    @property
    def sources(self) -> list[str]:
        """The normalized source paths, in order (a defensive copy)."""
        return list(self._sources)

    @property
    def encoding(self) -> str:
        """The text encoding used to decode each source."""
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

        Reads every source in order via :func:`read_csv`, concatenates the
        records in source order, then applies the transforms in order via
        :func:`apply_transforms`.

        Returns:
            The transformed records as a new list.

        Raises:
            IngestError: if a source cannot be read or is malformed.
            SchemaError: if a cell cannot be coerced to its inferred type.
            TransformError: if a transform fails.
        """
        records: list[dict[str, int | float | str]] = []
        for src in self._sources:
            _, src_records = read_csv(
                src, self._encoding, self._sample_size, schema=self._schema
            )
            records.extend(src_records)
        return apply_transforms(records, self._transforms)

    def stream(self) -> Iterator[dict[str, int | float | str]]:
        """Run the pipeline in streaming mode, lazily yielding records.

        Chains :func:`iter_rows` over each source in order (never
        materializing any source) and pipes the result through
        :func:`stream_transforms`. One record is pulled, transformed, and
        yielded at a time.

        Yields:
            One transformed record per surviving input record.

        Raises:
            IngestError: if a source cannot be read or is malformed.
            SchemaError: if a cell cannot be coerced to its inferred type.
            TransformError: if any transform is not streamable (e.g. a
                batch-only :class:`~pipeline.transform.Aggregate`) or fails.
        """

        def _chain_sources() -> Iterator[dict[str, int | float | str]]:
            for src in self._sources:
                yield from iter_rows(
                    src, self._encoding, self._sample_size, schema=self._schema
                )

        return stream_transforms(_chain_sources(), self._transforms)

    def schema(self) -> Schema:
        """Return the :class:`Schema` of the first source.

        When an explicit *schema* was supplied to the constructor, it is
        returned directly (the source of truth). Otherwise the schema is
        inferred: for a multi-source pipeline this is the schema of the
        *first* source only; later sources are not inspected. Reads the first
        source via :func:`read_csv` and returns the schema half of the returned
        tuple.

        Returns:
            The supplied :class:`Schema` when one was given, otherwise the
            inferred :class:`Schema` of the first source.

        Raises:
            IngestError: if the first source cannot be read or is malformed.
            SchemaError: if schema inference fails.
        """
        if self._schema is not None:
            # An explicit schema was supplied: it is the source of truth.
            return self._schema
        schema, _ = read_csv(self._sources[0], self._encoding, self._sample_size)
        return schema

    def to_csv(
        self,
        path: str,
        *,
        schema: Schema | None = None,
        encoding: str = "utf-8",
    ) -> int:
        """Run the pipeline in batch mode and write the records to *path*.

        Reads and transforms every source via :meth:`run`, then writes the
        records to *path* via :func:`~pipeline.output.write_csv`. When *schema*
        is ``None`` the pipeline's :meth:`schema` (the first source's schema)
        is used to fix the column order.

        Args:
            path: path of the CSV file to write.
            schema: optional schema fixing the column order.
            encoding: text encoding used to encode the file. Defaults to
                ``"utf-8"``.

        Returns:
            The number of data rows written.

        Raises:
            IngestError: if a source cannot be read or is malformed.
            SchemaError: if a cell cannot be coerced to its inferred type.
            TransformError: if a transform fails.
            OutputError: if the output cannot be written.
        """
        records = self.run()
        if schema is None:
            schema = self.schema()
        return write_csv(records, path, schema=schema, encoding=encoding)

    def stream_to_csv(
        self,
        path: str,
        *,
        schema: Schema | None = None,
        encoding: str = "utf-8",
    ) -> int:
        """Run the pipeline in streaming mode and write the records to *path*.

        Pipes :meth:`stream` through
        :func:`~pipeline.output.iter_write_csv` so records are written one at a
        time without full materialization. When *schema* is ``None`` the
        pipeline's :meth:`schema` (the first source's schema) is used to fix
        the column order. This method never calls :meth:`run`.

        Args:
            path: path of the CSV file to write.
            schema: optional schema fixing the column order.
            encoding: text encoding used to encode the file. Defaults to
                ``"utf-8"``.

        Returns:
            The number of data rows written (the final running count).

        Raises:
            IngestError: if a source cannot be read or is malformed.
            SchemaError: if a cell cannot be coerced to its inferred type.
            TransformError: if any transform is not streamable or fails.
            OutputError: if the output cannot be written.
        """
        if schema is None:
            schema = self.schema()
        count = 0
        for running in iter_write_csv(
            self.stream(), path, schema=schema, encoding=encoding
        ):
            count = running
        return count

    def validate(
        self, rules: Sequence[Callable[[dict, int], list[ValidationIssue]]]
    ) -> list[ValidationIssue]:
        """Run the pipeline in batch mode and validate the resulting records.

        Builds a :class:`~pipeline.validation.Validator` from *rules*, runs the
        pipeline via :meth:`run`, and returns the validator's issues for the
        final records in record-major order.

        Args:
            rules: an ordered sequence of per-record checkers (each with the
                signature ``check(record, row) -> list[ValidationIssue]``).

        Returns:
            A new list of :class:`~pipeline.validation.ValidationIssue` for the
            final records. Empty when no rule finds any issue.

        Raises:
            IngestError: if a source cannot be read or is malformed.
            SchemaError: if a cell cannot be coerced to its inferred type.
            TransformError: if a transform fails.
            ValidationError: if a rule is malformed (bad factory argument).
        """
        validator = Validator(rules)
        records = self.run()
        return validator.validate(records)

    def iter_validate(
        self, rules: Sequence[Callable[[dict, int], list[ValidationIssue]]]
    ) -> Iterator[ValidationIssue]:
        """Run the pipeline in streaming mode and validate records lazily.

        Builds a :class:`~pipeline.validation.Validator` from *rules* and pipes
        :meth:`stream` through :meth:`Validator.iter_validate`, yielding issues
        one at a time without materializing the source. This method never calls
        :meth:`run`.

        Args:
            rules: an ordered sequence of per-record checkers (each with the
                signature ``check(record, row) -> list[ValidationIssue]``).

        Yields:
            One :class:`~pipeline.validation.ValidationIssue` at a time, in
            record-major order.

        Raises:
            IngestError: if a source cannot be read or is malformed.
            SchemaError: if a cell cannot be coerced to its inferred type.
            TransformError: if any transform is not streamable or fails.
            ValidationError: if a rule is malformed (bad factory argument).
        """
        validator = Validator(rules)
        return validator.iter_validate(self.stream())
