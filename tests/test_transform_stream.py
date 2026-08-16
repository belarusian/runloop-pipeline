"""Tests for the pipeline streaming + composition phase (Cycle 4).

Covers:
- :func:`stream_transforms` laziness (no up-front materialization).
- Per-record application through the stream, in order.
- Record dropping when a step returns ``None``.
- :class:`TransformError` when a batch-only op (``Aggregate``) is passed to
  :func:`stream_transforms` or :func:`compose`.
- :func:`compose` ordering and purity.
- Equivalence: :func:`stream_transforms` over :func:`iter_rows` yields the
  same records as :func:`apply_transforms` over :func:`read_csv`.
"""

from __future__ import annotations

import pytest

from pipeline.errors import TransformError
from pipeline.ingest import iter_rows, read_csv
from pipeline.transform import (
    Aggregate,
    Composed,
    Filter,
    MapColumn,
    Rename,
    Select,
    Transform,
    apply_transforms,
    compose,
    stream_transforms,
)

# ---------------------------------------------------------------------------
# streamable flag
# ---------------------------------------------------------------------------


def test_transform_streamable_defaults_to_true():
    assert Transform.streamable is True


def test_per_record_ops_are_streamable():
    assert Filter(lambda r: True).streamable is True
    assert MapColumn("n", lambda v: v).streamable is True
    assert Rename("a", "b").streamable is True
    assert Select(["a"]).streamable is True


def test_aggregate_is_not_streamable():
    assert Aggregate(["g"], {"v": "sum"}).streamable is False


# ---------------------------------------------------------------------------
# stream_transforms laziness
# ---------------------------------------------------------------------------


def test_stream_transforms_is_lazy_and_does_not_materialize_source():
    """Calling next() once must not consume the whole source up front."""
    consumed = []

    def counting_source():
        for i in range(1000):
            consumed.append(i)
            yield {"n": i}

    stream = stream_transforms(counting_source(), [MapColumn("n", lambda v: v * 2)])
    first = next(stream)
    assert first == {"n": 0}
    # Only the first record was pulled from the source.
    assert consumed == [0]


def test_stream_transforms_pulls_exactly_one_record_per_next():
    consumed = []

    def counting_source():
        for i in range(5):
            consumed.append(i)
            yield {"n": i}

    stream = stream_transforms(counting_source(), [Filter(lambda r: True)])
    next(stream)
    assert consumed == [0]
    next(stream)
    assert consumed == [0, 1]


def test_stream_transforms_returns_a_generator():
    import types

    stream = stream_transforms(iter([{"n": 1}]), [Filter(lambda r: True)])
    assert isinstance(stream, types.GeneratorType)  # a generator object


# ---------------------------------------------------------------------------
# per-record application through the stream, in order
# ---------------------------------------------------------------------------


def test_stream_transforms_applies_each_transform_in_order():
    source = iter([{"n": 1, "name": "a"}, {"n": 2, "name": "b"}])
    transforms = [
        MapColumn("n", lambda v: v * 10),  # n=10, n=20
        Rename("name", "label"),
        Select(["n", "label"]),
    ]
    result = list(stream_transforms(source, transforms))
    assert result == [{"n": 10, "label": "a"}, {"n": 20, "label": "b"}]


def test_stream_transforms_empty_transforms_yields_source_unchanged():
    source = iter([{"n": 1}, {"n": 2}])
    assert list(stream_transforms(source, [])) == [{"n": 1}, {"n": 2}]


def test_stream_transforms_empty_source_yields_nothing():
    assert list(stream_transforms(iter([]), [Filter(lambda r: True)])) == []


# ---------------------------------------------------------------------------
# record dropping
# ---------------------------------------------------------------------------


def test_stream_transforms_drops_record_when_a_step_returns_none():
    source = iter([{"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}])
    # Keep only even n.
    result = list(stream_transforms(source, [Filter(lambda r: r["n"] % 2 == 0)]))
    assert result == [{"n": 2}, {"n": 4}]


def test_stream_transforms_drops_record_when_a_later_step_returns_none():
    source = iter([{"n": 1}, {"n": 2}, {"n": 3}])
    # Map n to n*10 (1->10, 2->20, 3->30), then keep only n < 25.
    # The n=2 -> 20 record survives; n=3 -> 30 is dropped by the later step.
    transforms = [
        MapColumn("n", lambda v: v * 10),
        Filter(lambda r: r["n"] < 25),
    ]
    result = list(stream_transforms(source, transforms))
    assert result == [{"n": 10}, {"n": 20}]


def test_stream_transforms_drops_all_when_every_record_is_filtered():
    source = iter([{"n": 1}, {"n": 2}])
    result = list(stream_transforms(source, [Filter(lambda r: r["n"] > 100)]))
    assert result == []


# ---------------------------------------------------------------------------
# TransformError for batch-only ops
# ---------------------------------------------------------------------------


def test_stream_transforms_rejects_batch_only_aggregate():
    source = iter([{"g": "x", "v": 1}])
    with pytest.raises(TransformError):
        # The error is raised lazily on first pull, not at construction.
        list(stream_transforms(source, [Aggregate(["g"], {"v": "sum"})]))


def test_stream_transforms_rejects_aggregate_even_if_not_first():
    source = iter([{"g": "x", "v": 1}])
    with pytest.raises(TransformError):
        list(
            stream_transforms(
                source,
                [Filter(lambda r: True), Aggregate(["g"], {"v": "sum"})],
            )
        )


def test_compose_rejects_batch_only_aggregate():
    with pytest.raises(TransformError):
        compose(Filter(lambda r: True), Aggregate(["g"], {"v": "sum"}))


def test_compose_rejects_aggregate_even_if_not_first():
    with pytest.raises(TransformError):
        compose(Aggregate(["g"], {"v": "sum"}), Filter(lambda r: True))


# ---------------------------------------------------------------------------
# compose ordering + purity
# ---------------------------------------------------------------------------


def test_compose_applies_members_in_order():
    composed = compose(
        MapColumn("n", lambda v: v * 10),
        Rename("n", "x"),
        Select(["x"]),
    )
    result = composed.apply([{"n": 1, "name": "a"}, {"n": 2, "name": "b"}])
    assert result == [{"x": 10}, {"x": 20}]


def test_compose_order_matters():
    records = [{"n": 1, "name": "a"}]
    # Map then rename works.
    ok = compose(MapColumn("n", lambda v: v * 2), Rename("n", "x")).apply(records)
    assert ok == [{"x": 2, "name": "a"}]
    # Rename then map on the original name fails (column no longer present).
    with pytest.raises(TransformError):
        compose(Rename("n", "x"), MapColumn("n", lambda v: v * 2)).apply(records)


def test_compose_apply_one_chains_and_drops_on_first_none():
    composed = compose(
        Filter(lambda r: r["n"] % 2 == 0),
        MapColumn("n", lambda v: v * 100),
    )
    assert composed.apply_one({"n": 1}) is None
    assert composed.apply_one({"n": 2}) == {"n": 200}


def test_compose_drops_record_in_batch_apply():
    composed = compose(Filter(lambda r: r["n"] % 2 == 0), MapColumn("n", lambda v: v * 10))
    assert composed.apply([{"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}]) == [{"n": 20}, {"n": 40}]


def test_compose_is_streamable_when_all_members_are():
    composed = compose(Filter(lambda r: True), MapColumn("n", lambda v: v))
    assert composed.streamable is True


def test_compose_is_streamable_and_usable_in_stream_transforms():
    composed = compose(
        MapColumn("n", lambda v: v * 10),
        Filter(lambda r: r["n"] >= 20),
    )
    source = iter([{"n": 1}, {"n": 2}, {"n": 3}])
    result = list(stream_transforms(source, [composed]))
    assert result == [{"n": 20}, {"n": 30}]


def test_compose_exposes_members_in_order():
    a = Filter(lambda r: True)
    b = MapColumn("n", lambda v: v)
    composed = compose(a, b)
    assert composed.transforms == (a, b)


def test_composed_can_be_constructed_directly():
    composed = Composed((Filter(lambda r: True), MapColumn("n", lambda v: v * 2)))
    assert composed.apply([{"n": 1}]) == [{"n": 2}]
    assert isinstance(composed, Transform)


def test_compose_does_not_mutate_input_records():
    composed = compose(MapColumn("n", lambda v: v * 100), Rename("n", "x"))
    records = [{"n": 1, "name": "a"}]
    composed.apply(records)
    assert records == [{"n": 1, "name": "a"}]


def test_composed_apply_returns_new_list():
    composed = compose(Filter(lambda r: True))
    records = [{"n": 1}]
    result = composed.apply(records)
    assert result is not records
    assert result == records


# ---------------------------------------------------------------------------
# Equivalence: stream over iter_rows == batch over read_csv
# ---------------------------------------------------------------------------


def test_stream_over_iter_rows_matches_batch_over_read_csv(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(
        "id,name,score\n"
        "1,alpha,10\n"
        "2,beta,20\n"
        "3,gamma,30\n"
        "4,delta,40\n",
        encoding="utf-8",
    )

    transforms = [
        Filter(lambda r: r["score"] >= 20),  # keep id 2,3,4
        MapColumn("score", lambda v: v * 2),
        Select(["id", "score"]),
    ]

    _, batch_records = read_csv(csv_path)
    expected = apply_transforms(batch_records, transforms)

    streamed = list(stream_transforms(iter_rows(csv_path), transforms))
    assert streamed == expected
    assert streamed == [
        {"id": 2, "score": 40},
        {"id": 3, "score": 60},
        {"id": 4, "score": 80},
    ]


def test_stream_over_iter_rows_with_compose_matches_batch(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(
        "id,name,score\n"
        "1,alpha,10\n"
        "2,beta,20\n"
        "3,gamma,30\n",
        encoding="utf-8",
    )

    composed = compose(
        Filter(lambda r: r["score"] > 10),
        MapColumn("score", lambda v: v + 100),
        Rename("name", "label"),
    )

    _, batch_records = read_csv(csv_path)
    expected = apply_transforms(batch_records, [composed])

    streamed = list(stream_transforms(iter_rows(csv_path), [composed]))
    assert streamed == expected
    assert streamed == [
        {"id": 2, "label": "beta", "score": 120},
        {"id": 3, "label": "gamma", "score": 130},
    ]


# ---------------------------------------------------------------------------
# Laziness with a real file: bounded memory (only pull what is asked)
# ---------------------------------------------------------------------------


def test_stream_over_iter_rows_is_lazy(tmp_path):
    csv_path = tmp_path / "data.csv"
    rows = [f"{i},name{i},{i}" for i in range(1000)]
    csv_path.write_text("id,name,score\n" + "\n".join(rows) + "\n", encoding="utf-8")

    stream = stream_transforms(iter_rows(csv_path), [Filter(lambda r: True)])
    first = next(stream)
    assert first == {"id": 0, "name": "name0", "score": 0}
    # We only pulled one record; the file is not fully materialized.
    second = next(stream)
    assert second == {"id": 1, "name": "name1", "score": 1}
    # Exhaust the rest to make sure the whole stream is well-formed.
    rest = list(stream)
    assert len(rest) == 998
    assert rest[-1] == {"id": 999, "name": "name999", "score": 999}


def test_stream_transforms_does_not_consume_source_before_first_next():
    """Constructing the generator must not pull from the source at all."""
    consumed = []

    def counting_source():
        for i in range(10):
            consumed.append(i)
            yield {"n": i}

    stream = stream_transforms(counting_source(), [Filter(lambda r: True)])
    assert consumed == []  # nothing pulled yet
    next(stream)
    assert consumed == [0]


def test_stream_transforms_accepts_a_plain_generator_source():
    """Sanity: a plain generator (not a list) works as the source."""
    gen = ({"n": i} for i in range(3))
    assert list(stream_transforms(gen, [MapColumn("n", lambda v: v + 1)])) == [
        {"n": 1},
        {"n": 2},
        {"n": 3},
    ]

