"""Schema model and type inference for the pipeline package.

A :class:`Schema` is an ordered collection of :class:`Column` objects, each
pairing a column name with a Python type. :func:`infer_schema` inspects sample
rows of string cells and classifies each column as ``int``, ``float``, or
``str``. :func:`coerce_value` converts a single string cell to a column's type.
"""

from __future__ import annotations

from dataclasses import dataclass

from pipeline.errors import SchemaError


@dataclass(frozen=True)
class Column:
    """A single named column with an associated Python type."""

    name: str
    type: type


@dataclass(frozen=True)
class Schema:
    """An ordered collection of columns describing a table's structure."""

    columns: tuple[Column, ...]

    def names(self) -> list[str]:
        """Return the column names in order."""
        return [column.name for column in self.columns]

    def column(self, name: str) -> Column:
        """Return the column with the given name.

        Raises:
            SchemaError: if no column has the given name.
        """
        for column in self.columns:
            if column.name == name:
                return column
        raise SchemaError(f"column {name!r} not found in schema")

    def types(self) -> dict[str, type]:
        """Return a mapping of column name to Python type."""
        return {column.name: column.type for column in self.columns}


def _parses_as_int(value: str) -> bool:
    """Return True if *value* parses as an ``int``."""
    try:
        int(value)
    except (ValueError, TypeError):
        return False
    return True


def _parses_as_float(value: str) -> bool:
    """Return True if *value* parses as a ``float``."""
    try:
        float(value)
    except (ValueError, TypeError):
        return False
    return True


def infer_schema(sample_rows: list[list[str]], header: list[str] | None = None) -> Schema:
    """Infer a :class:`Schema` from sample rows of string cells.

    Each column is classified as ``int`` if every non-empty cell parses as an
    ``int``, else ``float`` if every non-empty cell parses as a ``float``, else
    ``str``. A column with no non-empty cells is classified as ``str``.

    Args:
        sample_rows: a list of rows, each a list of string cells.
        header: optional column names. When omitted, names are generated as
            ``col_0``, ``col_1``, ... in order.

    Returns:
        The inferred :class:`Schema`.

    Raises:
        SchemaError: if *sample_rows* is empty.
    """
    if not sample_rows:
        raise SchemaError("cannot infer schema from an empty sample")

    width = max(len(row) for row in sample_rows)
    columns: list[Column] = []
    for index in range(width):
        non_empty = [row[index] for row in sample_rows if index < len(row) and row[index] != ""]
        if not non_empty:
            col_type: type = str
        elif all(_parses_as_int(cell) for cell in non_empty):
            col_type = int
        elif all(_parses_as_float(cell) for cell in non_empty):
            col_type = float
        else:
            col_type = str
        name = header[index] if header is not None else f"col_{index}"
        columns.append(Column(name=name, type=col_type))

    return Schema(columns=tuple(columns))


def coerce_value(value: str, col_type: type) -> int | float | str:
    """Coerce a string *value* to *col_type*.

    Args:
        value: the raw string cell.
        col_type: the target type (``int``, ``float``, or ``str``).

    Returns:
        The coerced value.

    Raises:
        SchemaError: if the value cannot be parsed as *col_type*, or if
            *col_type* is not a supported type.
    """
    if col_type is int:
        try:
            return int(value)
        except (ValueError, TypeError) as exc:
            raise SchemaError(f"cannot coerce {value!r} to int") from exc
    if col_type is float:
        try:
            return float(value)
        except (ValueError, TypeError) as exc:
            raise SchemaError(f"cannot coerce {value!r} to float") from exc
    if col_type is str:
        return value
    raise SchemaError(f"unsupported column type: {col_type!r}")
