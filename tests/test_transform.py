"""Tests for the pipeline Transformation phase."""

import copy

import pytest

from pipeline.errors import PipelineError, TransformError
from pipeline.transform import (
    Aggregate,
    Filter,
    MapColumn,
    Rename,
    Select,
    Transform,
    apply_transforms,
)

# ---------------------------------------------------------------------------
# Transform ABC
# ---------------------------------------------------------------------------


def test_transform_is_abstract():
    with pytest.raises(TypeError):
        Transform()  # cannot instantiate an ABC with an abstract method


def test_transform_default_apply_maps_apply_one_and_drops_none():
    class KeepEven(Transform):
        def apply_one(self, record):
            return record if record["n"] % 2 == 0 else None

    records = [{"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}]
    assert KeepEven().apply(records) == [{"n": 2}, {"n": 4}]


def test_transform_default_apply_returns_new_list():
    class Identity(Transform):
        def apply_one(self, record):
            return record

    records = [{"n": 1}, {"n": 2}]
    result = Identity().apply(records)
    assert result is not records
    assert result == records


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------


def test_filter_keeps_matching_records():
    records = [{"n": 1}, {"n": 2}, {"n": 3}]
    result = Filter(lambda r: r["n"] > 1).apply(records)
    assert result == [{"n": 2}, {"n": 3}]


def test_filter_drops_all_when_none_match():
    records = [{"n": 1}, {"n": 2}]
    assert Filter(lambda r: r["n"] > 10).apply(records) == []


def test_filter_keeps_all_when_all_match():
    records = [{"n": 1}, {"n": 2}]
    assert Filter(lambda r: True).apply(records) == [{"n": 1}, {"n": 2}]


def test_filter_apply_one_returns_record_or_none():
    op = Filter(lambda r: r["n"] > 1)
    assert op.apply_one({"n": 5}) == {"n": 5}
    assert op.apply_one({"n": 0}) is None


def test_filter_predicate_raising_raises_transform_error():
    def bad_predicate(record):
        raise ValueError("boom")

    with pytest.raises(TransformError) as excinfo:
        Filter(bad_predicate).apply_one({"n": 1})
    assert isinstance(excinfo.value.__cause__, ValueError)


def test_filter_predicate_raising_is_pipeline_error():
    with pytest.raises(PipelineError):
        Filter(lambda r: 1 / 0).apply_one({"n": 1})


# ---------------------------------------------------------------------------
# MapColumn
# ---------------------------------------------------------------------------


def test_map_column_rewrites_value_in_new_dict():
    records = [{"n": 1, "name": "a"}, {"n": 2, "name": "b"}]
    result = MapColumn("n", lambda v: v * 10).apply(records)
    assert result == [{"n": 10, "name": "a"}, {"n": 20, "name": "b"}]


def test_map_column_does_not_mutate_input_record():
    records = [{"n": 1, "name": "a"}]
    MapColumn("n", lambda v: v + 1).apply(records)
    assert records == [{"n": 1, "name": "a"}]


def test_map_column_missing_column_raises():
    with pytest.raises(TransformError):
        MapColumn("nope", lambda v: v).apply_one({"n": 1})


def test_map_column_fn_raising_raises_transform_error():
    def bad_fn(value):
        raise KeyError("bad")

    with pytest.raises(TransformError) as excinfo:
        MapColumn("n", bad_fn).apply_one({"n": 1})
    assert isinstance(excinfo.value.__cause__, KeyError)


def test_map_column_apply_one_returns_new_dict():
    record = {"n": 1}
    result = MapColumn("n", lambda v: v + 1).apply_one(record)
    assert result is not record
    assert result == {"n": 2}


# ---------------------------------------------------------------------------
# Rename
# ---------------------------------------------------------------------------


def test_rename_renames_column_in_new_dict():
    records = [{"a": 1, "b": 2}]
    result = Rename("a", "x").apply(records)
    assert result == [{"x": 1, "b": 2}]


def test_rename_preserves_other_columns():
    records = [{"a": 1, "b": 2, "c": 3}]
    result = Rename("b", "z").apply(records)
    assert result == [{"a": 1, "z": 2, "c": 3}]


def test_rename_does_not_mutate_input_record():
    records = [{"a": 1, "b": 2}]
    Rename("a", "x").apply(records)
    assert records == [{"a": 1, "b": 2}]


def test_rename_unknown_old_raises():
    with pytest.raises(TransformError):
        Rename("nope", "x").apply_one({"a": 1})


def test_rename_apply_one_returns_new_dict():
    record = {"a": 1}
    result = Rename("a", "x").apply_one(record)
    assert result is not record
    assert result == {"x": 1}


# ---------------------------------------------------------------------------
# Select
# ---------------------------------------------------------------------------


def test_select_keeps_only_named_columns_in_order():
    records = [{"a": 1, "b": 2, "c": 3}]
    result = Select(["c", "a"]).apply(records)
    assert result == [{"c": 3, "a": 1}]
    assert list(result[0].keys()) == ["c", "a"]


def test_select_single_column():
    records = [{"a": 1, "b": 2}]
    assert Select(["b"]).apply(records) == [{"b": 2}]


def test_select_does_not_mutate_input_record():
    records = [{"a": 1, "b": 2}]
    Select(["a"]).apply(records)
    assert records == [{"a": 1, "b": 2}]


def test_select_unknown_name_raises():
    with pytest.raises(TransformError):
        Select(["a", "nope"]).apply_one({"a": 1, "b": 2})


def test_select_apply_one_returns_new_dict():
    record = {"a": 1, "b": 2}
    result = Select(["a"]).apply_one(record)
    assert result is not record
    assert result == {"a": 1}


# ---------------------------------------------------------------------------
# Aggregate
# ---------------------------------------------------------------------------


def test_aggregate_single_group_count():
    records = [{"g": "x", "v": 1}, {"g": "x", "v": 2}]
    result = Aggregate(["g"], {"v": "count"}).apply(records)
    assert result == [{"g": "x", "v": 2}]


def test_aggregate_multiple_groups_first_seen_order():
    records = [
        {"g": "b", "v": 1},
        {"g": "a", "v": 10},
        {"g": "b", "v": 2},
        {"g": "a", "v": 20},
    ]
    result = Aggregate(["g"], {"v": "sum"}).apply(records)
    assert result == [{"g": "b", "v": 3}, {"g": "a", "v": 30}]


def test_aggregate_sum():
    records = [{"g": "x", "v": 1}, {"g": "x", "v": 2}, {"g": "x", "v": 3}]
    assert Aggregate(["g"], {"v": "sum"}).apply(records) == [{"g": "x", "v": 6}]


def test_aggregate_mean():
    records = [{"g": "x", "v": 1}, {"g": "x", "v": 2}, {"g": "x", "v": 3}]
    assert Aggregate(["g"], {"v": "mean"}).apply(records) == [{"g": "x", "v": 2.0}]


def test_aggregate_min_max():
    records = [{"g": "x", "v": 5}, {"g": "x", "v": 1}, {"g": "x", "v": 3}]
    assert Aggregate(["g"], {"v": "min"}).apply(records) == [{"g": "x", "v": 1}]
    assert Aggregate(["g"], {"v": "max"}).apply(records) == [{"g": "x", "v": 5}]


def test_aggregate_multiple_agg_columns():
    records = [
        {"g": "x", "v": 1, "w": 10},
        {"g": "x", "v": 3, "w": 20},
    ]
    result = Aggregate(["g"], {"v": "sum", "w": "mean"}).apply(records)
    assert result == [{"g": "x", "v": 4, "w": 15.0}]


def test_aggregate_multiple_group_by_columns():
    records = [
        {"a": 1, "b": "x", "v": 1},
        {"a": 1, "b": "x", "v": 2},
        {"a": 1, "b": "y", "v": 10},
    ]
    result = Aggregate(["a", "b"], {"v": "sum"}).apply(records)
    assert result == [{"a": 1, "b": "x", "v": 3}, {"a": 1, "b": "y", "v": 10}]


def test_aggregate_count_over_string_column():
    # 'count' is len(values) and works over any type, including str.
    records = [{"g": "x", "name": "a"}, {"g": "x", "name": "b"}]
    assert Aggregate(["g"], {"name": "count"}).apply(records) == [{"g": "x", "name": 2}]


def test_aggregate_numeric_over_string_raises():
    records = [{"g": "x", "name": "a"}, {"g": "x", "name": "b"}]
    for kind in ("sum", "mean", "min", "max"):
        with pytest.raises(TransformError):
            Aggregate(["g"], {"name": kind}).apply(records)


def test_aggregate_numeric_over_bool_raises():
    # bool is a subclass of int but must be rejected for numeric aggregations.
    records = [{"g": "x", "flag": True}, {"g": "x", "flag": False}]
    for kind in ("sum", "mean", "min", "max"):
        with pytest.raises(TransformError):
            Aggregate(["g"], {"flag": kind}).apply(records)


def test_aggregate_unknown_group_by_column_raises():
    records = [{"g": "x", "v": 1}]
    with pytest.raises(TransformError):
        Aggregate(["nope"], {"v": "sum"}).apply(records)


def test_aggregate_unknown_agg_column_raises():
    records = [{"g": "x", "v": 1}]
    with pytest.raises(TransformError):
        Aggregate(["g"], {"nope": "sum"}).apply(records)


def test_aggregate_unknown_kind_raises():
    with pytest.raises(TransformError):
        Aggregate(["g"], {"v": "median"})


def test_aggregate_apply_one_raises():
    with pytest.raises(TransformError, match="batch-only"):
        Aggregate(["g"], {"v": "sum"}).apply_one({"g": "x", "v": 1})


def test_aggregate_empty_records_returns_empty():
    assert Aggregate(["g"], {"v": "sum"}).apply([]) == []


def test_aggregate_does_not_mutate_input_records():
    records = [{"g": "x", "v": 1}, {"g": "x", "v": 2}]
    Aggregate(["g"], {"v": "sum"}).apply(records)
    assert records == [{"g": "x", "v": 1}, {"g": "x", "v": 2}]


# ---------------------------------------------------------------------------
# apply_transforms composition
# ---------------------------------------------------------------------------


def test_apply_transforms_applies_in_order():
    records = [{"n": 1, "name": "a"}, {"n": 2, "name": "b"}, {"n": 3, "name": "c"}]
    transforms = [
        Filter(lambda r: r["n"] > 1),  # keep n=2, n=3
        MapColumn("n", lambda v: v * 10),  # n=20, n=30
        Select(["n"]),  # drop name
    ]
    result = apply_transforms(records, transforms)
    assert result == [{"n": 20}, {"n": 30}]


def test_apply_transforms_order_matters():
    records = [{"n": 1, "name": "a"}]
    # Filter first (keeps n=1), then rename.
    first = apply_transforms(records, [Filter(lambda r: r["n"] == 1), Rename("n", "x")])
    assert first == [{"x": 1, "name": "a"}]
    # Rename first, then filter on the original name would fail to find it.
    with pytest.raises(TransformError):
        apply_transforms(records, [Rename("n", "x"), Filter(lambda r: r["n"] == 1)])


def test_apply_transforms_empty_transforms_returns_copy():
    records = [{"n": 1}]
    result = apply_transforms(records, [])
    assert result == records
    assert result is not records


def test_apply_transforms_returns_new_list():
    records = [{"n": 1}]
    result = apply_transforms(records, [Filter(lambda r: True)])
    assert result is not records


def test_apply_transforms_with_aggregate():
    records = [
        {"g": "x", "v": 1},
        {"g": "x", "v": 2},
        {"g": "y", "v": 10},
    ]
    result = apply_transforms(records, [Aggregate(["g"], {"v": "sum"})])
    assert result == [{"g": "x", "v": 3}, {"g": "y", "v": 10}]


# ---------------------------------------------------------------------------
# Purity: input list and record dicts are never mutated
# ---------------------------------------------------------------------------


def test_purity_filter_does_not_mutate_input():
    records = [{"n": 1}, {"n": 2}, {"n": 3}]
    snapshot = copy.deepcopy(records)
    Filter(lambda r: r["n"] > 1).apply(records)
    assert records == snapshot


def test_purity_map_column_does_not_mutate_input():
    records = [{"n": 1, "name": "a"}]
    snapshot = copy.deepcopy(records)
    MapColumn("n", lambda v: v * 100).apply(records)
    assert records == snapshot


def test_purity_rename_does_not_mutate_input():
    records = [{"a": 1, "b": 2}]
    snapshot = copy.deepcopy(records)
    Rename("a", "x").apply(records)
    assert records == snapshot


def test_purity_select_does_not_mutate_input():
    records = [{"a": 1, "b": 2}]
    snapshot = copy.deepcopy(records)
    Select(["a"]).apply(records)
    assert records == snapshot


def test_purity_aggregate_does_not_mutate_input():
    records = [{"g": "x", "v": 1}, {"g": "x", "v": 2}]
    snapshot = copy.deepcopy(records)
    Aggregate(["g"], {"v": "sum"}).apply(records)
    assert records == snapshot


def test_purity_apply_transforms_does_not_mutate_input():
    records = [
        {"n": 1, "name": "a"},
        {"n": 2, "name": "b"},
        {"n": 3, "name": "c"},
    ]
    snapshot = copy.deepcopy(records)
    apply_transforms(
        records,
        [Filter(lambda r: r["n"] > 1), MapColumn("n", lambda v: v + 1), Select(["n"])],
    )
    assert records == snapshot


def test_purity_apply_transforms_does_not_mutate_input_list_identity():
    records = [{"n": 1}, {"n": 2}]
    apply_transforms(records, [Filter(lambda r: True)])
    # The input list object itself is unchanged.
    assert records == [{"n": 1}, {"n": 2}]


# ---------------------------------------------------------------------------
# apply_one matches the per-record slice of apply for per-record ops
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "op,records",
    [
        (Filter(lambda r: r["n"] > 1), [{"n": 1}, {"n": 2}, {"n": 3}]),
        (MapColumn("n", lambda v: v * 2), [{"n": 1}, {"n": 2}]),
        (Rename("n", "x"), [{"n": 1, "m": 2}, {"n": 3, "m": 4}]),
        (Select(["n"]), [{"n": 1, "m": 2}, {"n": 3, "m": 4}]),
    ],
)
def test_apply_one_matches_per_record_slice_of_apply(op, records):
    expected = [result for result in (op.apply_one(r) for r in records) if result is not None]
    assert op.apply(records) == expected


def test_apply_one_matches_apply_for_filter_with_drops():
    op = Filter(lambda r: r["n"] % 2 == 0)
    records = [{"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}]
    per_record = [op.apply_one(r) for r in records]
    assert per_record == [None, {"n": 2}, None, {"n": 4}]
    assert op.apply(records) == [{"n": 2}, {"n": 4}]
