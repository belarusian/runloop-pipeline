"""Tests for the pipeline orchestration class (Cycle 5).

Covers:
- :meth:`Pipeline.run` end-to-end over a temp CSV with transforms.
- :meth:`Pipeline.run` with no transforms equals :func:`read_csv` records.
- :meth:`Pipeline.run` with a batch-only :class:`Aggregate`.
- :meth:`Pipeline.stream` laziness (a counting source / ``next()`` does not
  consume the whole file).
- :meth:`Pipeline.stream` over :func:`iter_rows` yields the same records as
  :meth:`Pipeline.run` for an equivalent CSV.
- :meth:`Pipeline.stream` with a batch-only :class:`Aggregate` raises
  :class:`TransformError`.
- :meth:`Pipeline.schema` returns the correct :class:`Schema`.
- The constructor defensively copies the transforms list.
"""

from __future__ import annotations

import types

import pytest

from pipeline.errors import TransformError
from pipeline.ingest import iter_rows, read_csv
from pipeline.pipeline import Pipeline
from pipeline.schema import Schema
from pipeline.transform import Aggregate, Filter, MapColumn, Rename, Select


def _write_csv(tmp_path, text: str, name: str = "data.csv"):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# run() end-to-end
# ---------------------------------------------------------------------------


def test_run_end_to_end_with_transforms(tmp_path):
    csv_path = _write_csv(
        tmp_path,
        "id,name,score\n"
        "1,alpha,10\n"
        "2,beta,20\n"
        "3,gamma,30\n"
        "4,delta,40\n",
    )
    pipeline = Pipeline(
        csv_path,
        [
            Filter(lambda r: r["score"] >= 20),  # keep id 2,3,4
            MapColumn("score", lambda v: v * 2),
            Rename("name", "label"),
            Select(["id", "label", "score"]),
        ],
    )

    result = pipeline.run()

    assert result == [
        {"id": 2, "label": "beta", "score": 40},
        {"id": 3, "label": "gamma", "score": 60},
        {"id": 4, "label": "delta", "score": 80},
    ]
    # Coercion is preserved through the transforms.
    assert all(isinstance(r["id"], int) for r in result)
    assert all(isinstance(r["score"], int) for r in result)


def test_run_with_no_transforms_equals_read_csv_records(tmp_path):
    csv_path = _write_csv(
        tmp_path,
        "id,name,score\n1,alice,9.5\n2,bob,8.0\n",
    )
    pipeline = Pipeline(csv_path)

    result = pipeline.run()
    _, expected = read_csv(csv_path)

    assert result == expected
    assert result == [
        {"id": 1, "name": "alice", "score": 9.5},
        {"id": 2, "name": "bob", "score": 8.0},
    ]


def test_run_with_batch_only_aggregate(tmp_path):
    csv_path = _write_csv(
        tmp_path,
        "group,value\n"
        "a,1\n"
        "a,2\n"
        "b,10\n",
    )
    pipeline = Pipeline(csv_path, [Aggregate(["group"], {"value": "sum"})])

    result = pipeline.run()

    assert result == [
        {"group": "a", "value": 3},
        {"group": "b", "value": 10},
    ]


def test_run_returns_a_new_list(tmp_path):
    csv_path = _write_csv(tmp_path, "n\n1\n2\n")
    pipeline = Pipeline(csv_path)
    result = pipeline.run()
    # Re-running yields an equal but distinct list.
    again = pipeline.run()
    assert result == again
    assert result is not again


# ---------------------------------------------------------------------------
# stream() laziness
# ---------------------------------------------------------------------------


def test_stream_is_lazy_and_does_not_consume_whole_file(tmp_path):
    # 1000 data rows. Pulling one record must not read the whole file.
    rows = [f"{i},name{i},{i}" for i in range(1000)]
    csv_path = _write_csv(tmp_path, "id,name,score\n" + "\n".join(rows) + "\n")

    pipeline = Pipeline(csv_path, [Filter(lambda r: True)])
    stream = pipeline.stream()

    first = next(stream)
    assert first == {"id": 0, "name": "name0", "score": 0}
    second = next(stream)
    assert second == {"id": 1, "name": "name1", "score": 1}
    # The stream is a genuine generator (lazy), not a materialized list.
    assert isinstance(stream, types.GeneratorType)
    # Drain the rest to confirm the whole stream is well-formed.
    rest = list(stream)
    assert len(rest) == 998
    assert rest[-1] == {"id": 999, "name": "name999", "score": 999}


def test_stream_is_lazy_via_counting_source(tmp_path, monkeypatch):
    """Pulling one record must not pull the whole source up front.

    We wrap :func:`iter_rows` to count how many records are *yielded* through
    the pipeline. If :meth:`Pipeline.stream` were eager, a single ``next()``
    would push every record through; laziness means only one is pulled.
    """
    csv_path = _write_csv(tmp_path, "n\n" + "\n".join(str(i) for i in range(1000)) + "\n")

    pulled: list[int] = []

    def counting_iter_rows(path, encoding, sample_size):
        for record in iter_rows(path, encoding, sample_size):
            pulled.append(record["n"])
            yield record

    monkeypatch.setattr("pipeline.pipeline.iter_rows", counting_iter_rows)

    pipeline = Pipeline(csv_path, [MapColumn("n", lambda v: v)])
    stream = pipeline.stream()
    first = next(stream)
    assert first == {"n": 0}
    # Only the first record was pulled from the source.
    assert pulled == [0]


def test_stream_returns_a_generator(tmp_path):
    csv_path = _write_csv(tmp_path, "n\n1\n2\n")
    pipeline = Pipeline(csv_path)
    assert isinstance(pipeline.stream(), types.GeneratorType)


# ---------------------------------------------------------------------------
# stream() equivalence with run()
# ---------------------------------------------------------------------------


def test_stream_over_iter_rows_matches_run(tmp_path):
    csv_path = _write_csv(
        tmp_path,
        "id,name,score\n"
        "1,alpha,10\n"
        "2,beta,20\n"
        "3,gamma,30\n"
        "4,delta,40\n",
    )
    transforms = [
        Filter(lambda r: r["score"] >= 20),
        MapColumn("score", lambda v: v * 2),
        Select(["id", "score"]),
    ]
    pipeline = Pipeline(csv_path, transforms)

    streamed = list(pipeline.stream())
    batched = pipeline.run()

    assert streamed == batched
    assert streamed == [
        {"id": 2, "score": 40},
        {"id": 3, "score": 60},
        {"id": 4, "score": 80},
    ]


def test_stream_with_no_transforms_matches_run(tmp_path):
    csv_path = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n")
    pipeline = Pipeline(csv_path)
    assert list(pipeline.stream()) == pipeline.run()


# ---------------------------------------------------------------------------
# stream() with a batch-only op raises TransformError
# ---------------------------------------------------------------------------


def test_stream_with_batch_only_aggregate_raises_transform_error(tmp_path):
    csv_path = _write_csv(tmp_path, "group,value\na,1\na,2\n")
    pipeline = Pipeline(csv_path, [Aggregate(["group"], {"value": "sum"})])

    with pytest.raises(TransformError):
        # The error is raised lazily on first pull, not at construction.
        list(pipeline.stream())


def test_stream_with_batch_only_aggregate_not_first_raises(tmp_path):
    csv_path = _write_csv(tmp_path, "group,value\na,1\na,2\n")
    pipeline = Pipeline(
        csv_path,
        [Filter(lambda r: True), Aggregate(["group"], {"value": "sum"})],
    )
    with pytest.raises(TransformError):
        list(pipeline.stream())


# ---------------------------------------------------------------------------
# schema()
# ---------------------------------------------------------------------------


def test_schema_returns_correct_schema(tmp_path):
    csv_path = _write_csv(
        tmp_path,
        "id,name,score\n1,alice,9.5\n2,bob,8.0\n",
    )
    pipeline = Pipeline(csv_path)

    schema = pipeline.schema()

    assert isinstance(schema, Schema)
    assert schema.names() == ["id", "name", "score"]
    assert schema.types() == {"id": int, "name": str, "score": float}


def test_schema_matches_read_csv_schema(tmp_path):
    csv_path = _write_csv(tmp_path, "a,b\n1,2.5\n3,4.5\n")
    pipeline = Pipeline(csv_path)
    expected, _ = read_csv(csv_path)
    assert pipeline.schema() == expected


# ---------------------------------------------------------------------------
# constructor defensive copy
# ---------------------------------------------------------------------------


def test_constructor_defensively_copies_transforms(tmp_path):
    csv_path = _write_csv(tmp_path, "n\n1\n2\n")
    transforms = [Filter(lambda r: True), MapColumn("n", lambda v: v)]
    pipeline = Pipeline(csv_path, transforms)

    # Mutating the caller's list must not affect the pipeline.
    transforms.clear()
    transforms.append(Aggregate(["n"], {"n": "sum"}))

    # The pipeline still holds its original two transforms.
    assert len(pipeline.transforms) == 2
    assert all(isinstance(t, (Filter, MapColumn)) for t in pipeline.transforms)
    # And it still runs correctly.
    assert pipeline.run() == [{"n": 1}, {"n": 2}]


def test_constructor_stores_source_encoding_sample_size(tmp_path):
    csv_path = _write_csv(tmp_path, "n\n1\n")
    pipeline = Pipeline(csv_path, encoding="utf-8", sample_size=5)

    assert pipeline.source == csv_path
    assert pipeline.encoding == "utf-8"
    assert pipeline.sample_size == 5


def test_constructor_defaults(tmp_path):
    csv_path = _write_csv(tmp_path, "n\n1\n")
    pipeline = Pipeline(csv_path)
    assert pipeline.encoding == "utf-8-sig"
    assert pipeline.sample_size == 1000
    assert pipeline.transforms == []
