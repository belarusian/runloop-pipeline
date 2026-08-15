"""CSV ingestion for the pipeline package.

:func:`read_csv` opens a CSV file, parses its header and data rows, infers a
typed :class:`~pipeline.schema.Schema`, and coerces every cell to its column
type. File I/O problems and malformed CSV (e.g. ragged rows) raise
:class:`~pipeline.errors.IngestError`; type problems raise
:class:`~pipeline.errors.SchemaError`.
"""

from __future__ import annotations

import csv
from pathlib import Path

from pipeline.errors import IngestError
from pipeline.schema import Schema, coerce_value, infer_schema


def read_csv(path: str | Path) -> tuple[Schema, list[dict[str, int | float | str]]]:
    """Read a CSV file and return its schema plus coerced records.

    Args:
        path: path to the CSV file.

    Returns:
        A tuple of the inferred :class:`Schema` and a list of records, where
        each record maps a column name to its coerced value.

    Raises:
        IngestError: if the file cannot be read, is empty, or has ragged rows.
        SchemaError: if a cell cannot be coerced to its inferred type.
    """
    try:
        with open(path, "r", newline="") as handle:
            rows = list(csv.reader(handle))
    except OSError as exc:
        raise IngestError(f"cannot read CSV file {path!r}: {exc}") from exc
    except csv.Error as exc:
        raise IngestError(f"malformed CSV file {path!r}: {exc}") from exc

    if not rows:
        raise IngestError(f"CSV file {path!r} is empty (no header row)")

    header = rows[0]
    data_rows = rows[1:]
    width = len(header)

    for row in data_rows:
        if len(row) != width:
            raise IngestError(
                f"ragged row in {path!r}: expected {width} fields, got {len(row)}"
            )

    schema = infer_schema(data_rows, header=header)

    records: list[dict[str, int | float | str]] = []
    for row in data_rows:
        record: dict[str, int | float | str] = {}
        for column, cell in zip(schema.columns, row):
            record[column.name] = coerce_value(cell, column.type)
        records.append(record)

    return schema, records
