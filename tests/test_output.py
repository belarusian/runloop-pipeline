"""Tests for the output stage (Cycle 6).

Covers:
- :func:`write_csv` round-trips a list of records (read back with
  :func:`read_csv` and compare).
- :func:`write_csv` header order from an explicit schema.
- :func:`write_csv` header order from the first-seen union of keys when no
  schema is given.
- :func:`write_csv` empty-records case (header only, returns 0).
- :func:`write_csv` raises :class:`OutputError` on an unwritable path.
- :func:`write_csv` renders int / float / str values.
- :func:`iter_write_csv` laziness (a counting source / ``next()`` does not
  consume the whole source).
- :func:`iter_write_csv` yields a running row count.
- :func:`iter_write_csv` writes the same file as :func:`write_csv` for an
  equivalent source.
- :func:`iter_write_csv` raises :class:`OutputError` on an unwritable path.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from pipeline.errors import OutputError
from pipeline.ingest import read_csv
from pipeline.output import iter_write_csv, write_csv
from pipeline.schema import Column, Schema


def _read_raw(path: Path) -> list[list[str]]:
    """Read a CSV file back as raw string cells (no coercion)."""
    with open(path, "r", newline="", encoding="utf-8") as handle:
        return list(csv.reader(handle))


# ---------------------------------------------------------------------------
# write_csv round-trip
# ---------------------------------------------------------------------------


def test_write_csv_round_trips_records(tmp_path):
    records = [
        {"id": 1, "name": "alice", "score": 9.5},
        {"id": 2, "name": "bob", "score": 8.0},
        {"id": 3, "name": "carol", "score": 7.25},
    ]
    path = tmp_path / "out.csv"

    written = write_csv(records, str(path))

    assert written == 3
    _, read_back = read_csv(str(path))
    assert read_back == records


def test_write_csv_returns_number_of_data_rows(tmp_path):
    records = [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}]
    path = tmp_path / "out.csv"
    assert write_csv(records, str(path)) == 4


# ---------------------------------------------------------------------------
# write_csv header order
# ---------------------------------------------------------------------------


def test_write_csv_header_from_explicit_schema(tmp_path):
    records = [
        {"a": 1, "b": 2, "c": 3},
        {"a": 4, "b": 5, "c": 6},
    ]
    # Schema orders the columns c, a, b (deliberately not the record order).
    schema = Schema(
        columns=(
            Column("c", int),
            Column("a", int),
            Column("b", int),
        )
    )
    path = tmp_path / "out.csv"

    write_csv(records, str(path), schema=schema)

    rows = _read_raw(path)
    assert rows[0] == ["c", "a", "b"]
    assert rows[1] == ["3", "1", "2"]
    assert rows[2] == ["6", "4", "5"]


def test_write_csv_header_from_first_seen_union(tmp_path):
    records = [
        {"a": 1, "b": 2},
        {"b": 3, "c": 4},
    ]
    path = tmp_path / "out.csv"

    write_csv(records, str(path))

    rows = _read_raw(path)
    # Union of keys in first-seen order: a, b (from rec 1), c (from rec 2).
    assert rows[0] == ["a", "b", "c"]
    # Missing keys render as an empty string.
    assert rows[1] == ["1", "2", ""]
    assert rows[2] == ["", "3", "4"]


# ---------------------------------------------------------------------------
# write_csv empty records
# ---------------------------------------------------------------------------


def test_write_csv_empty_records_writes_header_only_and_returns_zero(tmp_path):
    path = tmp_path / "out.csv"

    written = write_csv([], str(path))

    assert written == 0
    rows = _read_raw(path)
    # Only an (empty) header row is present.
    assert rows == [[]]


def test_write_csv_empty_records_with_schema(tmp_path):
    schema = Schema(columns=(Column("x", int), Column("y", str)))
    path = tmp_path / "out.csv"

    written = write_csv([], str(path), schema=schema)

    assert written == 0
    rows = _read_raw(path)
    assert rows == [["x", "y"]]


# ---------------------------------------------------------------------------
# write_csv value rendering
# ---------------------------------------------------------------------------


def test_write_csv_renders_int_float_str_values(tmp_path):
    records = [
        {"i": 42, "f": 3.14, "s": "hello"},
        {"i": -7, "f": 0.0, "s": ""},
    ]
    path = tmp_path / "out.csv"

    write_csv(records, str(path))

    rows = _read_raw(path)
    assert rows[0] == ["i", "f", "s"]
    assert rows[1] == ["42", "3.14", "hello"]
    assert rows[2] == ["-7", "0.0", ""]


# ---------------------------------------------------------------------------
# write_csv OutputError on unwritable path
# ---------------------------------------------------------------------------


def test_write_csv_raises_output_error_on_unwritable_path(tmp_path):
    # A path that is a directory cannot be opened for writing.
    directory = tmp_path / "subdir"
    directory.mkdir()

    with pytest.raises(OutputError):
        write_csv([{"a": 1}], str(directory))


def test_write_csv_output_error_is_pipeline_error(tmp_path):
    from pipeline.errors import PipelineError

    directory = tmp_path / "subdir"
    directory.mkdir()

    with pytest.raises(PipelineError):
        write_csv([{"a": 1}], str(directory))


# ---------------------------------------------------------------------------
# iter_write_csv laziness
# ---------------------------------------------------------------------------


def test_iter_write_csv_is_lazy_and_does_not_consume_whole_source(tmp_path):
    pulled: list[int] = []

    def counting_source():
        for i in range(1000):
            pulled.append(i)
            yield {"n": i}

    path = tmp_path / "out.csv"
    stream = iter_write_csv(counting_source(), str(path))

    first = next(stream)
    assert first == 1
    # Only the first record was pulled from the source.
    assert pulled == [0]


def test_iter_write_csv_yields_running_count(tmp_path):
    path = tmp_path / "out.csv"
    source = ({"n": i} for i in range(5))

    counts = list(iter_write_csv(source, str(path)))

    assert counts == [1, 2, 3, 4, 5]


def test_iter_write_csv_empty_source_yields_nothing(tmp_path):
    path = tmp_path / "out.csv"
    source = iter(())

    counts = list(iter_write_csv(source, str(path)))

    assert counts == []
    rows = _read_raw(path)
    assert rows == [[]]


# ---------------------------------------------------------------------------
# iter_write_csv equivalence with write_csv
# ---------------------------------------------------------------------------


def test_iter_write_csv_writes_same_file_as_write_csv(tmp_path):
    records = [
        {"id": 1, "name": "alice", "score": 9.5},
        {"id": 2, "name": "bob", "score": 8.0},
        {"id": 3, "name": "carol", "score": 7.25},
    ]
    batch_path = tmp_path / "batch.csv"
    stream_path = tmp_path / "stream.csv"

    write_csv(records, str(batch_path))
    list(iter_write_csv(iter(records), str(stream_path)))

    assert batch_path.read_text(encoding="utf-8") == stream_path.read_text(encoding="utf-8")


def test_iter_write_csv_with_explicit_schema(tmp_path):
    records = [
        {"a": 1, "b": 2, "c": 3},
        {"a": 4, "b": 5, "c": 6},
    ]
    schema = Schema(
        columns=(
            Column("c", int),
            Column("a", int),
            Column("b", int),
        )
    )
    path = tmp_path / "out.csv"

    list(iter_write_csv(iter(records), str(path), schema=schema))

    rows = _read_raw(path)
    assert rows[0] == ["c", "a", "b"]
    assert rows[1] == ["3", "1", "2"]
    assert rows[2] == ["6", "4", "5"]


# ---------------------------------------------------------------------------
# iter_write_csv OutputError on unwritable path
# ---------------------------------------------------------------------------


def test_iter_write_csv_raises_output_error_on_unwritable_path(tmp_path):
    directory = tmp_path / "subdir"
    directory.mkdir()

    with pytest.raises(OutputError):
        # The error surfaces on the first pull (when the file is opened).
        next(iter(iter_write_csv(iter([{"a": 1}]), str(directory))))
