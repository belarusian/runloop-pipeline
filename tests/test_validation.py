"""Tests for the validation stage (Cycle 7).

Covers:
- Each rule factory's happy path and failure path (missing column, wrong type
  including bool-not-int, out-of-range, not-in-allowed).
- A bad rule argument raising :class:`ValidationError`.
- :meth:`Validator.validate` collecting issues in record-major order across
  records and rules.
- :meth:`Validator.iter_validate` laziness (a counting source / ``next()`` does
  not consume the whole source) and equivalence with :meth:`validate`.
- Empty input producing no issues.
"""

from __future__ import annotations

import types

import pytest

from pipeline.errors import ValidationError
from pipeline.validation import (
    ValidationIssue,
    Validator,
    in_range,
    one_of,
    require_column,
    type_is,
)

# ---------------------------------------------------------------------------
# ValidationIssue model
# ---------------------------------------------------------------------------


def test_validation_issue_is_frozen_dataclass():
    issue = ValidationIssue(row=0, column="a", message="boom")
    assert issue.row == 0
    assert issue.column == "a"
    assert issue.message == "boom"
    with pytest.raises(Exception):
        issue.row = 1  # frozen


def test_validation_issue_column_can_be_none():
    issue = ValidationIssue(row=3, column=None, message="record-level")
    assert issue.column is None


# ---------------------------------------------------------------------------
# require_column
# ---------------------------------------------------------------------------


def test_require_column_flags_missing_column():
    check = require_column("id")
    issues = check({"name": "alice"}, 0)
    assert issues == [
        ValidationIssue(row=0, column="id", message="missing required column 'id'")
    ]


def test_require_column_no_issue_when_present():
    check = require_column("id")
    assert check({"id": 1}, 0) == []


def test_require_column_uses_row_index():
    check = require_column("id")
    issues = check({}, 7)
    assert issues[0].row == 7


def test_require_column_rejects_non_str_name():
    with pytest.raises(ValidationError):
        require_column(1)


# ---------------------------------------------------------------------------
# type_is
# ---------------------------------------------------------------------------


def test_type_is_flags_wrong_type():
    check = type_is("n", int)
    issues = check({"n": "oops"}, 0)
    assert len(issues) == 1
    assert issues[0].column == "n"
    assert issues[0].row == 0


def test_type_is_no_issue_when_correct_type():
    assert type_is("n", int)({"n": 5}, 0) == []
    assert type_is("n", float)({"n": 5.0}, 0) == []
    assert type_is("n", str)({"n": "x"}, 0) == []


def test_type_is_bool_is_not_int():
    # bool is a subclass of int in Python; the guard must reject it.
    assert type_is("n", int)({"n": True}, 0) != []
    assert type_is("n", int)({"n": False}, 0) != []
    assert type_is("n", float)({"n": True}, 0) != []


def test_type_is_accepts_tuple_of_types():
    check = type_is("n", (int, float))
    assert check({"n": 5}, 0) == []
    assert check({"n": 5.0}, 0) == []
    assert check({"n": "x"}, 0) != []
    # bool still not treated as int/float even in a tuple.
    assert check({"n": True}, 0) != []


def test_type_is_no_issue_when_column_absent():
    # Presence is require_column's job; a missing column is not a type issue.
    assert type_is("n", int)({}, 0) == []


def test_type_is_rejects_unsupported_type():
    with pytest.raises(ValidationError):
        type_is("n", complex)
    with pytest.raises(ValidationError):
        type_is("n", (int, complex))
    with pytest.raises(ValidationError):
        type_is("n", list)


def test_type_is_rejects_non_str_name():
    with pytest.raises(ValidationError):
        type_is(1, int)


# ---------------------------------------------------------------------------
# in_range
# ---------------------------------------------------------------------------


def test_in_range_flags_out_of_range():
    check = in_range("n", 0, 10)
    assert check({"n": 11}, 0) != []
    assert check({"n": -1}, 0) != []


def test_in_range_no_issue_when_in_range():
    check = in_range("n", 0, 10)
    assert check({"n": 0}, 0) == []
    assert check({"n": 10}, 0) == []
    assert check({"n": 5}, 0) == []


def test_in_range_respects_none_bounds():
    assert in_range("n", lo=0)({"n": 1000}, 0) == []
    assert in_range("n", lo=0)({"n": -1}, 0) != []
    assert in_range("n", hi=0)({"n": -1000}, 0) == []
    assert in_range("n", hi=0)({"n": 1}, 0) != []
    assert in_range("n")({"n": 1e9}, 0) == []


def test_in_range_flags_non_numeric():
    assert in_range("n", 0, 10)({"n": "x"}, 0) != []
    # bool is not numeric for range purposes.
    assert in_range("n", 0, 10)({"n": True}, 0) != []


def test_in_range_no_issue_when_column_absent():
    assert in_range("n", 0, 10)({}, 0) == []


def test_in_range_rejects_lo_gt_hi():
    with pytest.raises(ValidationError):
        in_range("n", 5, 1)


def test_in_range_rejects_non_str_name():
    with pytest.raises(ValidationError):
        in_range(1, 0, 10)


# ---------------------------------------------------------------------------
# one_of
# ---------------------------------------------------------------------------


def test_one_of_flags_value_not_in_allowed():
    check = one_of("c", ["a", "b"])
    assert check({"c": "z"}, 0) != []


def test_one_of_no_issue_when_value_in_allowed():
    check = one_of("c", ["a", "b"])
    assert check({"c": "a"}, 0) == []
    assert check({"c": "b"}, 0) == []


def test_one_of_no_issue_when_column_absent():
    assert one_of("c", ["a", "b"])({}, 0) == []


def test_one_of_accepts_tuple_allowed():
    check = one_of("c", (1, 2, 3))
    assert check({"c": 2}, 0) == []
    assert check({"c": 9}, 0) != []


def test_one_of_rejects_non_sequence_allowed():
    with pytest.raises(ValidationError):
        one_of("c", "abc")  # a bare str is not an allowed sequence
    with pytest.raises(ValidationError):
        one_of("c", 5)


def test_one_of_rejects_non_str_name():
    with pytest.raises(ValidationError):
        one_of(1, ["a"])


# ---------------------------------------------------------------------------
# Validator.validate — record-major ordering across records and rules
# ---------------------------------------------------------------------------


def test_validate_collects_issues_in_record_major_order():
    # Two rules, two records. Record 0 fails rule A only; record 1 fails both.
    rule_a = require_column("id")
    rule_b = in_range("score", 0, 10)
    validator = Validator([rule_a, rule_b])

    records = [
        {"id": 1, "score": 50},   # rule A ok, rule B fails (50 > 10)
        {"score": 5},             # rule A fails (no id), rule B ok
    ]
    issues = validator.validate(records)

    # Record-major: all of record 0's issues, then all of record 1's issues.
    assert [i.row for i in issues] == [0, 1]
    assert issues[0].column == "score"  # record 0, rule B
    assert issues[1].column == "id"     # record 1, rule A


def test_validate_preserves_rule_order_within_record():
    # Both rules fail on the same record; rule order is preserved.
    rule_a = require_column("id")
    rule_b = require_column("name")
    validator = Validator([rule_a, rule_b])
    issues = validator.validate([{}])
    assert [i.column for i in issues] == ["id", "name"]


def test_validate_returns_a_new_list():
    validator = Validator([require_column("id")])
    first = validator.validate([{}])
    second = validator.validate([{}])
    assert first == second
    assert first is not second


def test_validate_empty_input_no_issues():
    validator = Validator([require_column("id"), in_range("n", 0, 1)])
    assert validator.validate([]) == []


def test_validate_all_passing_no_issues():
    validator = Validator([require_column("id"), type_is("n", int)])
    assert validator.validate([{"id": 1, "n": 2}]) == []


def test_validate_defensive_copy_of_rules():
    rules = [require_column("id")]
    validator = Validator(rules)
    rules.clear()
    # The validator still holds its original rule.
    assert validator.validate([{}]) != []


# ---------------------------------------------------------------------------
# Validator.iter_validate — laziness + equivalence
# ---------------------------------------------------------------------------


def test_iter_validate_is_lazy_and_does_not_consume_whole_source():
    pulled: list[int] = []

    def counting_source():
        for i in range(1000):
            pulled.append(i)
            yield {"id": i, "score": i}

    validator = Validator([require_column("id")])
    stream = validator.iter_validate(counting_source())
    assert isinstance(stream, types.GeneratorType)

    # No issues for these records, so pull until we force a second record.
    # Use a rule that fails on every record to observe pulls.
    pulled.clear()

    def counting_source_failing():
        for i in range(1000):
            pulled.append(i)
            yield {"score": i}  # missing id

    validator2 = Validator([require_column("id")])
    stream2 = validator2.iter_validate(counting_source_failing())
    first = next(stream2)
    assert first.row == 0
    # Only the first record was pulled from the source.
    assert pulled == [0]


def test_iter_validate_equivalence_with_validate():
    rules = [require_column("id"), in_range("score", 0, 10), type_is("tag", str)]
    records = [
        {"id": 1, "score": 5, "tag": "x"},
        {"score": 50, "tag": "y"},
        {"id": 3, "score": 2, "tag": 1},
        {},
    ]
    validator = Validator(rules)
    batched = validator.validate(records)
    streamed = list(validator.iter_validate(iter(records)))
    assert streamed == batched


def test_iter_validate_empty_source_no_issues():
    validator = Validator([require_column("id")])
    assert list(validator.iter_validate(iter([]))) == []


def test_iter_validate_row_counter_tracks_position():
    validator = Validator([require_column("id")])
    records = [{"id": 1}, {}, {"id": 3}, {}]
    issues = list(validator.iter_validate(iter(records)))
    assert [i.row for i in issues] == [1, 3]
