"""Output stage for the pipeline package.

This module is the write counterpart of :mod:`pipeline.ingest`. It persists
transformed records (``dict[str, int | float | str]``) back to a CSV file.

Two entry points are provided:

- :func:`write_csv` — a batch writer that takes a materialized sequence of
  records, writes a header row plus one row per record, and returns the number
  of data rows written.
- :func:`iter_write_csv` — a lazy streaming writer (a generator) that consumes
  an :class:`~collections.abc.Iterator` of records one at a time, writing each
  row as it is pulled and yielding a running row count. The source is never
  fully materialized.

Column order is taken from an explicit :class:`~pipeline.schema.Schema` when
one is supplied; otherwise it is derived from the records themselves (the
union of keys in first-seen order for :func:`write_csv`, or the keys of the
first record for :func:`iter_write_csv`). Every value is rendered via
``str()``; a column missing from a record renders as an empty string.

Failure contract: every failure in this module raises
:class:`~pipeline.errors.OutputError`, never a bare ``Exception``.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator, Sequence

from pipeline.errors import OutputError
from pipeline.schema import Schema


def _column_order(records: Sequence[dict], schema: Schema | None) -> list[str]:
    """Return the output column order for a batch of *records*.

    When *schema* is given, its :meth:`Schema.names` are used. Otherwise the
    union of keys across *records* is returned in first-seen order (an empty
    list when *records* is empty).
    """
    if schema is not None:
        return list(schema.names())
    header: list[str] = []
    seen: set[str] = set()
    for record in records:
        for key in record:
            if key not in seen:
                seen.add(key)
                header.append(key)
    return header


def _render(record: dict, name: str) -> str:
    """Render a single cell: ``str(value)`` if present, else an empty string."""
    if name not in record:
        return ""
    return str(record[name])


def _row(record: dict, header: list[str]) -> list[str]:
    """Render one *record* as an ordered list of cells for *header*."""
    return [_render(record, name) for name in header]


def write_csv(
    records: Sequence[dict],
    path: str,
    *,
    schema: Schema | None = None,
    encoding: str = "utf-8",
) -> int:
    """Write *records* to *path* as a CSV file and return the row count.

    A header row is written first, then one row per record. The column order
    is taken from *schema* when supplied, else from the union of the records'
    keys in first-seen order (an empty header when *records* is empty). Each
    value is rendered via ``str()``; a column missing from a record renders as
    an empty string.

    Args:
        records: the records to write, in order.
        path: path of the CSV file to write.
        schema: optional schema fixing the column order.
        encoding: text encoding used to encode the file. Defaults to
            ``"utf-8"``.

    Returns:
        The number of data rows written (i.e. ``len(records)``).

    Raises:
        OutputError: if the file cannot be opened, a value cannot be encoded,
            or the CSV writer fails.
    """
    header = _column_order(records, schema)
    try:
        with open(path, "w", newline="", encoding=encoding) as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            count = 0
            for record in records:
                writer.writerow(_row(record, header))
                count += 1
            return count
    except (OSError, UnicodeEncodeError, ValueError) as exc:
        raise OutputError(f"cannot write CSV to {path!r}: {exc}") from exc


def iter_write_csv(
    source: Iterator[dict],
    path: str,
    *,
    schema: Schema | None = None,
    encoding: str = "utf-8",
) -> Iterator[int]:
    """Lazily stream *source* to *path* as CSV, yielding a running row count.

    This is a generator: it opens *path*, writes the header, then consumes
    *source* one record at a time, writing each row and yielding the running
    data-row count (``1, 2, 3, ...``) after each write. The source is never
    fully materialized — each :func:`next` pulls exactly one record.

    The header is taken from *schema* when supplied; otherwise it is the keys
    of the first record (an empty header when *source* is empty). Each value is
    rendered via ``str()``; a column missing from a record renders as an empty
    string.

    Args:
        source: an iterator yielding records, in order.
        path: path of the CSV file to write.
        schema: optional schema fixing the column order.
        encoding: text encoding used to encode the file. Defaults to
            ``"utf-8"``.

    Yields:
        The running count of data rows written after each row is written.

    Raises:
        OutputError: if the file cannot be opened, a value cannot be encoded,
            or the CSV writer fails.
    """
    try:
        handle = open(path, "w", newline="", encoding=encoding)
    except OSError as exc:
        raise OutputError(f"cannot write CSV to {path!r}: {exc}") from exc

    try:
        writer = csv.writer(handle)
        it = iter(source)
        first: dict | None = None
        if schema is None:
            # Derive the header from the first record's keys.
            try:
                first = next(it)
            except StopIteration:
                first = None
            header = list(first.keys()) if first is not None else []
        else:
            header = list(schema.names())

        writer.writerow(header)
        count = 0
        if first is not None:
            writer.writerow(_row(first, header))
            count += 1
            yield count
        for record in it:
            writer.writerow(_row(record, header))
            count += 1
            yield count
    except (OSError, UnicodeEncodeError, ValueError) as exc:
        raise OutputError(f"cannot write CSV to {path!r}: {exc}") from exc
    finally:
        handle.close()
