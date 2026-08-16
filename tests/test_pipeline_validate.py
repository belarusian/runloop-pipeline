"""Tests for Pipeline.validate and Pipeline.iter_validate (Cycle 7).

Covers:
- :meth:`Pipeline.validate` returns issues for a temp CSV with a failing rule.
- :meth:`Pipeline.validate` with all-passing rules returns ``[]``.
- :meth:`Pipeline.iter_validate` laziness + order + equivalence with
  :meth:`validate`.
- :meth:`Pipeline.iter_validate` never calls :meth:`Pipeline.run` (spied via
  ``monkeypatch.setattr``).
- Validation over a multi-source pipeline.
"""

from __future__ import annotations

import types

from pipeline.pipeline import Pipeline
from pipeline.validation import (
    ValidationIssue,
    in_range,
    require_column,
    type_is,
)


def _write_csv(tmp_path, text: str, name: str = "data.csv") -> str:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return str(path)


# ---------------------------------------------------------------------------
# validate() — batch
# ---------------------------------------------------------------------------


def test_validate_returns_issues_for_failing_rule(tmp_path):
    csv_path = _write_csv(
        tmp_path,
        "id,score\n"
        "1,10\n"
        "2,150\n"
        "3,5\n",
    )
    pipeline = Pipeline(csv_path)

    issues = pipeline.validate([in_range("score", 0, 100)])

    assert issues == [
        ValidationIssue(
            row=1,
            column="score",
            message="column 'score' has value 150 outside range [0, 100]",
        )
    ]


def test_validate_all_passing_returns_empty(tmp_path):
    csv_path = _write_csv(tmp_path, "id,score\n1,10\n2,20\n")
    pipeline = Pipeline(csv_path)

    issues = pipeline.validate([require_column("id"), in_range("score", 0, 100)])

    assert issues == []


def test_validate_applies_transforms_before_validation(tmp_path):
    # A transform drops the offending row, so validation sees no issue.
    from pipeline.transform import Filter

    csv_path = _write_csv(tmp_path, "id,score\n1,10\n2,150\n")
    pipeline = Pipeline(csv_path, [Filter(lambda r: r["score"] <= 100)])

    issues = pipeline.validate([in_range("score", 0, 100)])

    assert issues == []


def test_validate_multiple_rules_record_major_order(tmp_path):
    csv_path = _write_csv(
        tmp_path,
        "id,score\n"
        "1,10\n"
        "2,150\n",
    )
    pipeline = Pipeline(csv_path)

    issues = pipeline.validate([require_column("id"), in_range("score", 0, 100)])

    # Record 0 passes both; record 1 fails only the range rule.
    assert [i.row for i in issues] == [1]
    assert issues[0].column == "score"


# ---------------------------------------------------------------------------
# iter_validate() — streaming
# ---------------------------------------------------------------------------


def test_iter_validate_is_lazy_and_does_not_consume_whole_source(tmp_path, monkeypatch):
    # 1000 data rows; pulling one issue must not read the whole file.
    rows = [f"{i},{i}" for i in range(1000)]
    csv_path = _write_csv(tmp_path, "id,score\n" + "\n".join(rows) + "\n")

    pulled: list[int] = []
    import pipeline.pipeline as pp

    original = pp.iter_rows

    def spy_iter_rows(path, encoding, sample_size, *, schema=None):
        for record in original(path, encoding, sample_size, schema=schema):
            pulled.append(record["id"])
            yield record

    monkeypatch.setattr(pp, "iter_rows", spy_iter_rows)

    pipeline = Pipeline(csv_path)
    # A rule that fails on every row so we can observe pulls via issues.
    stream = pipeline.iter_validate([require_column("nope")])
    assert isinstance(stream, types.GeneratorType)

    first = next(stream)
    assert first.row == 0
    # Only the first record was pulled from the source.
    assert pulled == [0]


def test_iter_validate_equivalence_with_validate(tmp_path):
    csv_path = _write_csv(
        tmp_path,
        "id,score\n"
        "1,10\n"
        "2,150\n"
        "3,5\n",
    )
    pipeline = Pipeline(csv_path)
    rules = [require_column("id"), in_range("score", 0, 100), type_is("id", int)]

    batched = pipeline.validate(rules)
    streamed = list(pipeline.iter_validate(rules))

    assert streamed == batched
    assert [i.row for i in streamed] == [1]


def test_iter_validate_empty_source_no_issues(tmp_path):
    # A filter that drops every record yields an empty stream.
    from pipeline.transform import Filter

    csv_path = _write_csv(tmp_path, "id,score\n1,10\n2,20\n")
    pipeline = Pipeline(csv_path, [Filter(lambda r: False)])

    assert list(pipeline.iter_validate([require_column("id")])) == []


def test_iter_validate_never_calls_run(tmp_path, monkeypatch):
    csv_path = _write_csv(tmp_path, "id,score\n1,10\n2,150\n")
    pipeline = Pipeline(csv_path)

    run_calls: list[int] = []

    def spy_run(self):
        run_calls.append(1)
        raise AssertionError("iter_validate must not call run()")

    monkeypatch.setattr(Pipeline, "run", spy_run)

    issues = list(pipeline.iter_validate([in_range("score", 0, 100)]))

    assert run_calls == []  # run() was never invoked
    assert [i.row for i in issues] == [1]


def test_iter_validate_multi_source(tmp_path):
    a = _write_csv(tmp_path, "id,score\n1,10\n2,150\n", "a.csv")
    b = _write_csv(tmp_path, "id,score\n3,200\n4,5\n", "b.csv")

    pipeline = Pipeline([a, b])
    rules = [in_range("score", 0, 100)]

    batched = pipeline.validate(rules)
    streamed = list(pipeline.iter_validate(rules))

    # Row indices are global across concatenated sources: rows 1 and 2 fail.
    assert streamed == batched
    assert [i.row for i in streamed] == [1, 2]
    assert [i.column for i in streamed] == ["score", "score"]


def test_validate_multi_source(tmp_path):
    a = _write_csv(tmp_path, "id,score\n1,10\n2,150\n", "a.csv")
    b = _write_csv(tmp_path, "id,score\n3,200\n4,5\n", "b.csv")

    pipeline = Pipeline([a, b])
    issues = pipeline.validate([in_range("score", 0, 100)])

    assert [i.row for i in issues] == [1, 2]
