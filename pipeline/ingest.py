"""CSV ingestion for the pipeline package.

:func:`read_csv` opens a CSV file, parses its header and data rows, infers a
typed :class:`~pipeline.schema.Schema`, and coerces every cell to its column
type. :func:`iter_rows` is a streaming variant that lazily yields one coerced
record at a time, inferring the schema from a bounded sample so large files
need not be fully materialized.

Both entry points accept an optional explicit *schema*. When one is supplied it
is used verbatim as the source of truth: no sampling or inference is performed,
ragged rows are validated against the supplied schema's width, and every row is
coerced against it. When *schema* is ``None`` the historical inference behavior
is preserved unchanged.

File I/O problems, undecodable bytes, and malformed CSV (e.g. ragged rows)
raise :class:`~pipeline.errors.IngestError`; type problems raise
:class:`~pipeline.errors.SchemaError`.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from itertools import islice
from pathlib import Path

from pipeline.errors import IngestError
from pipeline.schema import Column, Schema, coerce_value, infer_schema


def _coerce_row(row: list[str], schema: Schema) -> dict[str, int | float | str]:
    """Coerce a single CSV *row* into a record keyed by column name."""
    record: dict[str, int | float | str] = {}
    for column, cell in zip(schema.columns, row):
        record[column.name] = coerce_value(cell, column.type)
    return record


def _schema_from_header(header: list[str]) -> Schema:
    """Build a :class:`Schema` from a header row, typing every column as ``str``.

    Used for a header-only source (a header row with no data rows), where there
    is no data to infer column types from, so every column defaults to ``str``.

    Args:
        header: the header row (a list of column names).

    Returns:
        A :class:`Schema` whose columns are the header names, each typed ``str``.
    """
    return Schema(columns=tuple(Column(name=h, type=str) for h in header))


def read_csv(
    path: str | Path,
    encoding: str = "utf-8-sig",
    sample_size: int | None = 1000,
    *,
    schema: Schema | None = None,
) -> tuple[Schema, list[dict[str, int | float | str]]]:
    """Read a CSV file and return its schema plus coerced records.

    Args:
        path: path to the CSV file.
        encoding: text encoding used to decode the file. Defaults to
            ``"utf-8-sig"``, which transparently strips a leading UTF-8 BOM and
            is a no-op for plain UTF-8.
        sample_size: bound on how many leading data rows feed schema inference.
            Only the first *sample_size* rows are inspected to classify column
            types; coercion is still applied to every row. ``None`` inspects
            all rows. Ignored when an explicit *schema* is supplied.
        schema: an optional explicit :class:`Schema` to use verbatim as the
            source of truth. When supplied, no sampling or inference is
            performed: ragged rows are validated against the supplied schema's
            width and every row is coerced against it. The same *schema* object
            the caller passed is returned. When ``None`` (the default), the
            schema is inferred from the data as before.

    Returns:
        A tuple of the :class:`Schema` (the supplied one when *schema* is given,
        otherwise the inferred one) and a list of records, where each record
        maps a column name to its coerced value.

    Raises:
        IngestError: if the file cannot be read, cannot be decoded, is empty,
            or has ragged rows.
        SchemaError: if a cell cannot be coerced to its column type.
    """
    try:
        with open(path, "r", newline="", encoding=encoding) as handle:
            rows = list(csv.reader(handle))
    except OSError as exc:
        raise IngestError(f"cannot read CSV file {path!r}: {exc}") from exc
    except (UnicodeDecodeError, ValueError) as exc:
        raise IngestError(f"cannot decode CSV file {path!r}: {exc}") from exc
    except csv.Error as exc:
        raise IngestError(f"malformed CSV file {path!r}: {exc}") from exc

    if not rows:
        raise IngestError(f"CSV file {path!r} is empty (no header row)")

    data_rows = rows[1:]

    if schema is not None:
        # Explicit schema: use it verbatim. No sampling or inference. Validate
        # ragged rows against the supplied width and coerce every row against
        # the supplied schema. Return the same schema object the caller passed.
        width = len(schema.columns)
        for row in data_rows:
            if len(row) != width:
                raise IngestError(
                    f"ragged row in {path!r}: expected {width} fields, got {len(row)}"
                )
        records: list[dict[str, int | float | str]] = [
            _coerce_row(row, schema) for row in data_rows
        ]
        return schema, records

    header = rows[0]
    width = len(header)

    for row in data_rows:
        if len(row) != width:
            raise IngestError(
                f"ragged row in {path!r}: expected {width} fields, got {len(row)}"
            )

    if not data_rows:
        # Header-only source: there is no data to infer types from, so every
        # column is typed str and there are no records to return.
        return _schema_from_header(header), []

    schema = infer_schema(data_rows, header=header, sample_size=sample_size)

    records = []
    for row in data_rows:
        records.append(_coerce_row(row, schema))

    return schema, records


def iter_rows(
    path: str | Path,
    encoding: str = "utf-8-sig",
    sample_size: int | None = 1000,
    *,
    schema: Schema | None = None,
) -> Iterator[dict[str, int | float | str]]:
    """Lazily yield coerced records from a CSV file, one at a time.

    The header is read first, the schema is inferred from a bounded sample of
    the first *sample_size* data rows, and then every row (sample and the rest)
    is coerced and yielded as it is read. This keeps peak memory bounded by the
    sample rather than the whole file.

    When an explicit *schema* is supplied it is used verbatim: no sampling or
    inference is performed, ragged rows are validated against the supplied
    schema's width, and every row is coerced against it. A header-only source
    (no data rows) yields nothing.

    Args:
        path: path to the CSV file.
        encoding: text encoding used to decode the file. Defaults to
            ``"utf-8-sig"``, which strips a leading UTF-8 BOM and is a no-op
            for plain UTF-8.
        sample_size: number of leading data rows used to infer the schema.
            Coercion is applied to every row regardless of this bound. Ignored
            when an explicit *schema* is supplied.
        schema: an optional explicit :class:`Schema` to use verbatim as the
            source of truth. When supplied, no sampling or inference is
            performed. When ``None`` (the default), the schema is inferred from
            a bounded sample as before.

    Yields:
        One coerced record (a ``{column name: value}`` dict) per data row.

    Raises:
        IngestError: if the file cannot be read, cannot be decoded, is empty,
            or has ragged rows.
        SchemaError: if a cell cannot be coerced to its column type.
    """
    try:
        handle = open(path, "r", newline="", encoding=encoding)
    except OSError as exc:
        raise IngestError(f"cannot read CSV file {path!r}: {exc}") from exc

    try:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            raise IngestError(f"CSV file {path!r} is empty (no header row)")

        if schema is not None:
            # Explicit schema: use it verbatim. No sampling or inference.
            # Validate ragged rows against the supplied width and coerce every
            # row against the supplied schema. Header-only yields nothing.
            width = len(schema.columns)
            for row in reader:
                if len(row) != width:
                    raise IngestError(
                        f"ragged row in {path!r}: expected {width} fields, got {len(row)}"
                    )
                yield _coerce_row(row, schema)
            return

        width = len(header)

        sample: list[list[str]] = []
        for row in islice(reader, sample_size):
            if len(row) != width:
                raise IngestError(
                    f"ragged row in {path!r}: expected {width} fields, got {len(row)}"
                )
            sample.append(row)

        if not sample:
            # Header-only source: no data rows to infer from, so the schema is
            # built from the header (every column str) and nothing is yielded.
            return

        schema = infer_schema(sample, header=header, sample_size=sample_size)

        for row in sample:
            yield _coerce_row(row, schema)

        for row in reader:
            if len(row) != width:
                raise IngestError(
                    f"ragged row in {path!r}: expected {width} fields, got {len(row)}"
                )
            yield _coerce_row(row, schema)
    except (UnicodeDecodeError, ValueError) as exc:
        raise IngestError(f"cannot decode CSV file {path!r}: {exc}") from exc
    except csv.Error as exc:
        raise IngestError(f"malformed CSV file {path!r}: {exc}") from exc
    finally:
        handle.close()
