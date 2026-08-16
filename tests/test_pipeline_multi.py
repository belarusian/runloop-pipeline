"""Tests for multi-source Pipeline support and the output stage (Cycle 6).

Covers:
- Multi-source :meth:`Pipeline.run` concatenates sources in order.
- Multi-source :meth:`Pipeline.stream` laziness + order + equivalence with
  :meth:`run`.
- Multi-source :meth:`Pipeline.schema` returns the first source's schema.
- Single-source behavior is unchanged (regression).
- The constructor defensively copies the sources list.
- :meth:`Pipeline.to_csv` round-trip (batch) and :meth:`Pipeline.stream_to_csv`
  round-trip (streaming) over a temp CSV.
- :meth:`Pipeline.to_csv` / :meth:`Pipeline.stream_to_csv` with an explicit
  schema for stable column order.
"""

from __future__ import annotations

import csv
import types

import pytest

from pipeline.ingest import iter_rows, read_csv
from pipeline.pipeline import Pipeline
from pipeline.schema import Column, Schema
from pipeline.transform import Filter, MapColumn, Select


def _write_csv(tmp_path, text: str, name: str) -> str:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return str(path)


# ---------------------------------------------------------------------------
# multi-source run() concatenates in order
# ---------------------------------------------------------------------------


def test_run_multi_source_concatenates_in_order(tmp_path):
    a = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n", "a.csv")
    b = _write_csv(tmp_path, "id,name\n3,carol\n4,dave\n", "b.csv")

    pipeline = Pipeline([a, b])
    result = pipeline.run()

    assert result == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 3, "name": "carol"},
        {"id": 4, "name": "dave"},
    ]


def test_run_multi_source_with_transforms(tmp_path):
    a = _write_csv(tmp_path, "id,score\n1,10\n2,20\n", "a.csv")
    b = _write_csv(tmp_path, "id,score\n3,30\n4,40\n", "b.csv")

    pipeline = Pipeline(
        [a, b],
        [Filter(lambda r: r["score"] >= 20), MapColumn("score", lambda v: v * 2)],
    )
    result = pipeline.run()

    assert result == [
        {"id": 2, "score": 40},
        {"id": 3, "score": 60},
        {"id": 4, "score": 80},
    ]


# ---------------------------------------------------------------------------
# multi-source stream() laziness + order + equivalence
# ---------------------------------------------------------------------------


def test_stream_multi_source_is_lazy(tmp_path, monkeypatch):
    a = _write_csv(tmp_path, "n\n1\n2\n", "a.csv")
    b = _write_csv(tmp_path, "n\n3\n4\n", "b.csv")

    pulled: list[int] = []

    def counting_iter_rows(path, encoding, sample_size, *, schema=None):
        for record in iter_rows(path, encoding, sample_size, schema=schema):
            pulled.append(record["n"])
            yield record

    monkeypatch.setattr("pipeline.pipeline.iter_rows", counting_iter_rows)

    pipeline = Pipeline([a, b])
    stream = pipeline.stream()
    first = next(stream)
    assert first == {"n": 1}
    # Only the first record of the first source was pulled.
    assert pulled == [1]


def test_stream_multi_source_order_and_equivalence_with_run(tmp_path):
    a = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n", "a.csv")
    b = _write_csv(tmp_path, "id,name\n3,carol\n4,dave\n", "b.csv")

    pipeline = Pipeline([a, b])
    streamed = list(pipeline.stream())
    batched = pipeline.run()

    assert streamed == batched
    assert streamed == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 3, "name": "carol"},
        {"id": 4, "name": "dave"},
    ]


def test_stream_multi_source_returns_a_generator(tmp_path):
    a = _write_csv(tmp_path, "n\n1\n", "a.csv")
    b = _write_csv(tmp_path, "n\n2\n", "b.csv")
    pipeline = Pipeline([a, b])
    assert isinstance(pipeline.stream(), types.GeneratorType)


# ---------------------------------------------------------------------------
# multi-source schema() returns the first source's schema
# ---------------------------------------------------------------------------


def test_schema_multi_source_returns_first_source_schema(tmp_path):
    # The two sources have different column sets; schema() must reflect only
    # the first source.
    a = _write_csv(tmp_path, "id,name\n1,alice\n", "a.csv")
    b = _write_csv(tmp_path, "id,score\n2,9.5\n", "b.csv")

    pipeline = Pipeline([a, b])
    schema = pipeline.schema()

    assert isinstance(schema, Schema)
    assert schema.names() == ["id", "name"]
    assert schema.types() == {"id": int, "name": str}


# ---------------------------------------------------------------------------
# single-source regression
# ---------------------------------------------------------------------------


def test_single_source_str_behavior_unchanged(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n", "data.csv")

    pipeline = Pipeline(csv_path)

    # source property returns the original str for a single source.
    assert pipeline.source == csv_path
    assert pipeline.run() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]
    assert list(pipeline.stream()) == pipeline.run()
    assert pipeline.schema().names() == ["id", "name"]


def test_single_source_list_of_one_matches_str(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n", "data.csv")

    as_str = Pipeline(csv_path)
    as_list = Pipeline([csv_path])

    assert as_str.run() == as_list.run()
    # A one-element list still reports the bare str via the source property.
    assert as_list.source == csv_path


# ---------------------------------------------------------------------------
# constructor defensive copy of sources
# ---------------------------------------------------------------------------


def test_constructor_defensively_copies_sources_list(tmp_path):
    a = _write_csv(tmp_path, "n\n1\n", "a.csv")
    b = _write_csv(tmp_path, "n\n2\n", "b.csv")
    sources = [a, b]

    pipeline = Pipeline(sources)

    # Mutating the caller's list must not affect the pipeline.
    sources.clear()
    sources.append("nonexistent.csv")

    assert pipeline.sources == [a, b]
    assert pipeline.run() == [{"n": 1}, {"n": 2}]


def test_constructor_rejects_empty_source_sequence(tmp_path):
    with pytest.raises(Exception):
        Pipeline([])


# ---------------------------------------------------------------------------
# to_csv round-trip (batch)
# ---------------------------------------------------------------------------


def test_to_csv_round_trip(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name,score\n1,alice,9.5\n2,bob,8.0\n", "data.csv")
    out_path = tmp_path / "out.csv"

    # A transform that keeps every column so the inferred schema and the
    # record keys stay aligned for a clean round-trip.
    pipeline = Pipeline(csv_path, [MapColumn("score", lambda v: v)])
    written = pipeline.to_csv(str(out_path))

    assert written == 2
    _, read_back = read_csv(str(out_path))
    assert read_back == [
        {"id": 1, "name": "alice", "score": 9.5},
        {"id": 2, "name": "bob", "score": 8.0},
    ]


def test_to_csv_multi_source_round_trip(tmp_path):
    a = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n", "a.csv")
    b = _write_csv(tmp_path, "id,name\n3,carol\n4,dave\n", "b.csv")
    out_path = tmp_path / "out.csv"

    pipeline = Pipeline([a, b])
    written = pipeline.to_csv(str(out_path))

    assert written == 4
    _, read_back = read_csv(str(out_path))
    assert read_back == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 3, "name": "carol"},
        {"id": 4, "name": "dave"},
    ]


# ---------------------------------------------------------------------------
# stream_to_csv round-trip (streaming)
# ---------------------------------------------------------------------------


def test_stream_to_csv_round_trip(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name,score\n1,alice,9.5\n2,bob,8.0\n", "data.csv")
    out_path = tmp_path / "out.csv"

    pipeline = Pipeline(csv_path, [MapColumn("score", lambda v: v)])
    written = pipeline.stream_to_csv(str(out_path))

    assert written == 2
    _, read_back = read_csv(str(out_path))
    assert read_back == [
        {"id": 1, "name": "alice", "score": 9.5},
        {"id": 2, "name": "bob", "score": 8.0},
    ]


def test_stream_to_csv_multi_source_round_trip(tmp_path):
    a = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n", "a.csv")
    b = _write_csv(tmp_path, "id,name\n3,carol\n4,dave\n", "b.csv")
    out_path = tmp_path / "out.csv"

    pipeline = Pipeline([a, b])
    written = pipeline.stream_to_csv(str(out_path))

    assert written == 4
    _, read_back = read_csv(str(out_path))
    assert read_back == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 3, "name": "carol"},
        {"id": 4, "name": "dave"},
    ]


def test_stream_to_csv_does_not_call_run(tmp_path, monkeypatch):
    csv_path = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n", "data.csv")
    out_path = tmp_path / "out.csv"

    calls = {"run": 0}

    def spy_run(self):
        calls["run"] += 1
        raise AssertionError("run() must not be called by stream_to_csv")

    monkeypatch.setattr(Pipeline, "run", spy_run)

    pipeline = Pipeline(csv_path)
    written = pipeline.stream_to_csv(str(out_path))

    assert written == 2
    assert calls["run"] == 0


# ---------------------------------------------------------------------------
# to_csv / stream_to_csv with an explicit schema for stable column order
# ---------------------------------------------------------------------------


def test_to_csv_with_explicit_schema_stable_column_order(tmp_path):
    csv_path = _write_csv(tmp_path, "a,b,c\n1,2,3\n4,5,6\n", "data.csv")
    out_path = tmp_path / "out.csv"

    # The transform reorders the record keys; the explicit schema fixes the
    # output column order independently of the record key order.
    schema = Schema(
        columns=(
            Column("c", int),
            Column("a", int),
            Column("b", int),
        )
    )
    pipeline = Pipeline(csv_path)
    pipeline.to_csv(str(out_path), schema=schema)

    with open(out_path, "r", newline="", encoding="utf-8") as handle:
        import csv as _csv

        rows = list(_csv.reader(handle))

    assert rows[0] == ["c", "a", "b"]
    assert rows[1] == ["3", "1", "2"]
    assert rows[2] == ["6", "4", "5"]


def test_stream_to_csv_with_explicit_schema_stable_column_order(tmp_path):
    csv_path = _write_csv(tmp_path, "a,b,c\n1,2,3\n4,5,6\n", "data.csv")
    out_path = tmp_path / "out.csv"

    schema = Schema(
        columns=(
            Column("c", int),
            Column("a", int),
            Column("b", int),
        )
    )
    pipeline = Pipeline(csv_path)
    pipeline.stream_to_csv(str(out_path), schema=schema)

    with open(out_path, "r", newline="", encoding="utf-8") as handle:
        import csv as _csv

        rows = list(_csv.reader(handle))

    assert rows[0] == ["c", "a", "b"]
    assert rows[1] == ["3", "1", "2"]
    assert rows[2] == ["6", "4", "5"]


def test_to_csv_and_stream_to_csv_produce_identical_files(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name,score\n1,alice,9.5\n2,bob,8.0\n", "data.csv")
    batch_path = tmp_path / "batch.csv"
    stream_path = tmp_path / "stream.csv"

    pipeline = Pipeline(csv_path)
    pipeline.to_csv(str(batch_path))
    pipeline.stream_to_csv(str(stream_path))

    assert batch_path.read_text(encoding="utf-8") == stream_path.read_text(encoding="utf-8")


def test_to_csv_uses_pipeline_schema_when_none_given(tmp_path):
    # Without an explicit schema, to_csv uses the first source's schema, so
    # the column order matches the source header even after a transform that
    # reorders record keys.
    csv_path = _write_csv(tmp_path, "a,b,c\n1,2,3\n4,5,6\n", "data.csv")
    out_path = tmp_path / "out.csv"

    pipeline = Pipeline(csv_path, [Select(["c", "a", "b"])])
    pipeline.to_csv(str(out_path))

    with open(out_path, "r", newline="", encoding="utf-8") as handle:
        import csv as _csv

        rows = list(_csv.reader(handle))

    # Header follows the source schema (a, b, c), not the record key order.
    assert rows[0] == ["a", "b", "c"]
    assert rows[1] == ["1", "2", "3"]
    assert rows[2] == ["4", "5", "6"]


# ---------------------------------------------------------------------------
# multi-source explicit schema: stable column order + empty-string rendering
# for columns absent from some records (issue #43)
# ---------------------------------------------------------------------------


def test_multi_source_explicit_schema_stable_order_and_empty_string_rendering(tmp_path):
    # Two sources with *different* column sets: source A has `name`, source B
    # has `score`. Neither source has both columns.
    a = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n", "a.csv")
    b = _write_csv(tmp_path, "id,score\n3,9.5\n4,8.0\n", "b.csv")

    # The explicit schema declares all three columns in a fixed order.
    schema = Schema(
        columns=(
            Column("id", int),
            Column("name", str),
            Column("score", float),
        )
    )

    batch_path = tmp_path / "batch.csv"
    stream_path = tmp_path / "stream.csv"

    pipeline = Pipeline([a, b])
    assert pipeline.to_csv(str(batch_path), schema=schema) == 4
    assert pipeline.stream_to_csv(str(stream_path), schema=schema) == 4

    # Both writers produce the same stable column order and render a column
    # that is absent from a record as an empty string.
    for path in (batch_path, stream_path):
        with open(path, "r", newline="", encoding="utf-8") as handle:
            rows = list(csv.reader(handle))
        assert rows == [
            ["id", "name", "score"],
            ["1", "alice", ""],
            ["2", "bob", ""],
            ["3", "", "9.5"],
            ["4", "", "8.0"],
        ]
    # Batch and streaming outputs are byte-identical.
    assert batch_path.read_bytes() == stream_path.read_bytes()
