"""Tests for the explicit schema override on :class:`Pipeline` (issues #49-#52).

Covers:
- ``Pipeline(source, schema=...)`` pins the supplied types across
  :meth:`run`, :meth:`stream`, :meth:`to_csv`, and :meth:`stream_to_csv`.
- :meth:`Pipeline.schema` returns the supplied schema when one was given.
- ``schema=None`` (the default) still infers as before (regression).
- Streaming laziness is preserved when an explicit schema is supplied.
"""

from __future__ import annotations

import types

import pytest

from pipeline.pipeline import Pipeline
from pipeline.schema import Column, Schema
from pipeline.transform import MapColumn


def _write_csv(tmp_path, text: str, name: str = "data.csv"):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def _schema(*cols: tuple[str, type]) -> Schema:
    return Schema(columns=tuple(Column(name=n, type=t) for n, t in cols))


# ---------------------------------------------------------------------------
# schema() returns the supplied schema
# ---------------------------------------------------------------------------


def test_schema_returns_supplied_schema(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n")
    supplied = _schema(("id", str), ("name", str))
    pipeline = Pipeline(csv_path, schema=supplied)

    assert pipeline.schema() is supplied


def test_schema_none_still_infers(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n")
    pipeline = Pipeline(csv_path)

    schema = pipeline.schema()
    assert isinstance(schema, Schema)
    # Inferred: digits -> int, not pinned to str.
    assert schema.types() == {"id": int, "name": str}


# ---------------------------------------------------------------------------
# run() pins the supplied types
# ---------------------------------------------------------------------------


def test_run_pins_supplied_types(tmp_path):
    # id is digits-only (would infer int) but the supplied schema pins it to str.
    csv_path = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n")
    supplied = _schema(("id", str), ("name", str))
    pipeline = Pipeline(csv_path, schema=supplied)

    records = pipeline.run()
    assert records == [{"id": "1", "name": "alice"}, {"id": "2", "name": "bob"}]
    assert all(isinstance(r["id"], str) for r in records)


def test_run_with_transforms_pins_supplied_types(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n")
    supplied = _schema(("id", str), ("name", str))
    pipeline = Pipeline(
        csv_path,
        [MapColumn("name", lambda v: v.upper())],
        schema=supplied,
    )

    records = pipeline.run()
    assert records == [{"id": "1", "name": "ALICE"}, {"id": "2", "name": "BOB"}]
    assert all(isinstance(r["id"], str) for r in records)


def test_run_schema_none_regression(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n")
    pipeline = Pipeline(csv_path)

    records = pipeline.run()
    assert records == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    assert all(isinstance(r["id"], int) for r in records)


# ---------------------------------------------------------------------------
# stream() pins the supplied types and stays lazy
# ---------------------------------------------------------------------------


def test_stream_pins_supplied_types(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n")
    supplied = _schema(("id", str), ("name", str))
    pipeline = Pipeline(csv_path, schema=supplied)

    records = list(pipeline.stream())
    assert records == [{"id": "1", "name": "alice"}, {"id": "2", "name": "bob"}]
    assert all(isinstance(r["id"], str) for r in records)


def test_stream_is_lazy_with_explicit_schema(tmp_path):
    # 1000 rows; pulling one record must not read the whole file.
    rows = [f"{i},name{i}" for i in range(1000)]
    csv_path = _write_csv(tmp_path, "id,name\n" + "\n".join(rows) + "\n")
    supplied = _schema(("id", int), ("name", str))

    pulled: list[int] = []
    import pipeline.pipeline as pp

    original = pp.iter_rows

    def spy_iter_rows(path, encoding, sample_size, *, schema=None):
        for record in original(path, encoding, sample_size, schema=schema):
            pulled.append(record["id"])
            yield record

    saved = pp.iter_rows
    pp.iter_rows = spy_iter_rows
    try:
        pipeline = Pipeline(csv_path, schema=supplied)
        stream = pipeline.stream()
        assert isinstance(stream, types.GeneratorType)
        first = next(stream)
        assert first == {"id": 0, "name": "name0"}
        # Only the first record was pulled from the source.
        assert pulled == [0]
    finally:
        pp.iter_rows = saved


# ---------------------------------------------------------------------------
# to_csv() pins the supplied types
# ---------------------------------------------------------------------------


def test_to_csv_pins_supplied_types(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n")
    out_path = tmp_path / "out.csv"
    supplied = _schema(("id", str), ("name", str))
    pipeline = Pipeline(csv_path, schema=supplied)

    count = pipeline.to_csv(out_path)
    assert count == 2
    text = out_path.read_text(encoding="utf-8")
    # id values are written as the pinned str values (no float/int reformatting).
    assert text.splitlines() == ["id,name", "1,alice", "2,bob"]


def test_to_csv_schema_none_regression(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n")
    out_path = tmp_path / "out.csv"
    pipeline = Pipeline(csv_path)

    count = pipeline.to_csv(out_path)
    assert count == 2
    assert out_path.read_text(encoding="utf-8").splitlines() == [
        "id,name",
        "1,alice",
        "2,bob",
    ]


# ---------------------------------------------------------------------------
# stream_to_csv() pins the supplied types
# ---------------------------------------------------------------------------


def test_stream_to_csv_pins_supplied_types(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n")
    out_path = tmp_path / "out.csv"
    supplied = _schema(("id", str), ("name", str))
    pipeline = Pipeline(csv_path, schema=supplied)

    count = pipeline.stream_to_csv(out_path)
    assert count == 2
    assert out_path.read_text(encoding="utf-8").splitlines() == [
        "id,name",
        "1,alice",
        "2,bob",
    ]


def test_stream_to_csv_schema_none_regression(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n")
    out_path = tmp_path / "out.csv"
    pipeline = Pipeline(csv_path)

    count = pipeline.stream_to_csv(out_path)
    assert count == 2
    assert out_path.read_text(encoding="utf-8").splitlines() == [
        "id,name",
        "1,alice",
        "2,bob",
    ]


# ---------------------------------------------------------------------------
# explicit schema threads through multi-source run()
# ---------------------------------------------------------------------------


def test_run_multi_source_pins_supplied_types(tmp_path):
    a = _write_csv(tmp_path, "id,name\n1,alice\n", "a.csv")
    b = _write_csv(tmp_path, "id,name\n2,bob\n", "b.csv")
    supplied = _schema(("id", str), ("name", str))
    pipeline = Pipeline([a, b], schema=supplied)

    records = pipeline.run()
    assert records == [{"id": "1", "name": "alice"}, {"id": "2", "name": "bob"}]
    assert all(isinstance(r["id"], str) for r in records)


# ---------------------------------------------------------------------------
# explicit schema with a coercion failure surfaces SchemaError
# ---------------------------------------------------------------------------


def test_run_explicit_schema_coercion_failure_raises(tmp_path):
    csv_path = _write_csv(tmp_path, "value\n1\nx\n")
    supplied = _schema(("value", int))
    pipeline = Pipeline(csv_path, schema=supplied)

    from pipeline.errors import SchemaError

    with pytest.raises(SchemaError):
        pipeline.run()
