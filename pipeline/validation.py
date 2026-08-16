"""Validation stage for the pipeline package.

The validation stage inspects records (``dict[str, int | float | str]``) and
reports *issues* — a missing column, a wrong type, an out-of-range value, or a
value not in an allowed set. Issues are **returned as data**, never raised:
each is a :class:`ValidationIssue` carrying the 0-based ``row`` index, the
``column`` name (``None`` for record-level issues), and a human-readable
``message``.

Rule factories
--------------
Each factory below returns a *per-record checker* with the signature
``check(record: dict, row: int) -> list[ValidationIssue]``:

- :func:`require_column` — flag a record that lacks a required column.
- :func:`type_is` — flag a present value whose type is not as expected
  (``bool`` is deliberately *not* treated as an ``int``/``float``).
- :func:`in_range` — flag a present value that is non-numeric or outside
  ``[lo, hi]``.
- :func:`one_of` — flag a present value that is not in an allowed set.

Every factory validates its *own* arguments and raises
:class:`~pipeline.errors.ValidationError` on a bad argument (e.g. a
non-string column name, an unsupported expected type, ``lo > hi``, or a
non-sequence allowed set).

:class:`Validator`
------------------
Holds a defensive copy of an ordered sequence of checkers and exposes:

- :meth:`Validator.validate` — batch: run every rule over every record and
  return a new list of issues in record-major order (all rules for record 0,
  then all rules for record 1, ...).
- :meth:`Validator.iter_validate` — streaming: a lazy generator that pulls one
  record at a time from an iterator and yields issues one at a time, never
  materializing the source.

Failure contract: this module never raises a bare ``Exception``. Malformed
rules raise :class:`~pipeline.errors.ValidationError`; validation *issues* are
returned as data.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass

from pipeline.errors import ValidationError

#: A per-record checker: given a record and its 0-based row index, return the
#: list of issues it finds in that record.
Rule = Callable[[dict, int], list["ValidationIssue"]]

#: The Python types accepted by :func:`type_is`.
_ALLOWED_TYPES = (int, float, str)


@dataclass(frozen=True)
class ValidationIssue:
    """A single validation finding, returned as data (never raised).

    Attributes:
        row: the 0-based index of the offending record in the input.
        column: the offending column name, or ``None`` for record-level issues.
        message: a human-readable description of the problem.
    """

    row: int
    column: str | None
    message: str


def _require_str_name(name: object, factory: str) -> str:
    """Return *name* if it is a ``str``; otherwise raise ``ValidationError``."""
    if not isinstance(name, str):
        raise ValidationError(f"{factory}: column name must be a str, got {type(name).__name__}")
    return name


def _is_numeric(value: object) -> bool:
    """True for ``int``/``float`` values, but *not* for ``bool``."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _type_matches(value: object, expected_type: type | tuple[type, ...]) -> bool:
    """Return whether *value* matches *expected_type*.

    ``bool`` is deliberately not treated as an ``int`` or ``float``: a boolean
    only matches when ``bool`` is explicitly the (or one of the) expected
    type(s).
    """
    if isinstance(value, bool):
        if isinstance(expected_type, tuple):
            return bool in expected_type
        return expected_type is bool
    return isinstance(value, expected_type)


def require_column(name: str) -> Rule:
    """Build a checker that flags records missing the column *name*.

    Args:
        name: the required column name.

    Returns:
        A per-record checker. For a record lacking *name* it yields one issue
        (``column=name``); for a record that has *name* it yields no issue.

    Raises:
        ValidationError: if *name* is not a ``str``.
    """
    _require_str_name(name, "require_column")

    def check(record: dict, row: int) -> list[ValidationIssue]:
        if name not in record:
            return [
                ValidationIssue(
                    row=row,
                    column=name,
                    message=f"missing required column {name!r}",
                )
            ]
        return []

    return check


def type_is(name: str, expected_type: type | tuple[type, ...]) -> Rule:
    """Build a checker that flags a present value whose type is not expected.

    Only records that *have* the column are inspected; a missing column is
    :func:`require_column`'s job and produces no issue here. ``bool`` is not
    treated as an ``int`` or ``float``.

    Args:
        name: the column name to check.
        expected_type: an expected type — one of ``int``/``float``/``str``, or
            a tuple of those.

    Returns:
        A per-record checker. For a record where *name* is present and the
        value does not match *expected_type* it yields one issue
        (``column=name``); otherwise no issue.

    Raises:
        ValidationError: if *name* is not a ``str`` or *expected_type* is not
            one of ``int``/``float``/``str`` (or a tuple of those).
    """
    _require_str_name(name, "type_is")
    if isinstance(expected_type, tuple):
        for t in expected_type:
            if t not in _ALLOWED_TYPES:
                raise ValidationError(
                    f"type_is: unsupported expected type {t!r}; "
                    f"must be one of {list(_ALLOWED_TYPES)} or a tuple of those"
                )
    elif expected_type not in _ALLOWED_TYPES:
        raise ValidationError(
            f"type_is: unsupported expected type {expected_type!r}; "
            f"must be one of {list(_ALLOWED_TYPES)} or a tuple of those"
        )

    def check(record: dict, row: int) -> list[ValidationIssue]:
        if name not in record:
            return []
        value = record[name]
        if not _type_matches(value, expected_type):
            return [
                ValidationIssue(
                    row=row,
                    column=name,
                    message=(
                        f"column {name!r} has value {value!r} which is not "
                        f"an instance of {expected_type!r}"
                    ),
                )
            ]
        return []

    return check


def in_range(name: str, lo: int | float | None = None, hi: int | float | None = None) -> Rule:
    """Build a checker that flags a present value outside ``[lo, hi]``.

    Only records that *have* the column are inspected; a missing column
    produces no issue here. The value must be numeric (``int`` or ``float``,
    not ``bool``); a non-numeric value is flagged, and so is a numeric value
    outside the (optionally one-sided) range.

    Args:
        name: the column name to check.
        lo: inclusive lower bound, or ``None`` for no lower bound.
        hi: inclusive upper bound, or ``None`` for no upper bound.

    Returns:
        A per-record checker. For a record where *name* is present and the
        value is non-numeric or out of range it yields one issue
        (``column=name``); otherwise no issue.

    Raises:
        ValidationError: if *name* is not a ``str`` or ``lo > hi``.
    """
    _require_str_name(name, "in_range")
    if lo is not None and hi is not None and lo > hi:
        raise ValidationError(f"in_range: lower bound {lo!r} must not exceed upper bound {hi!r}")

    def check(record: dict, row: int) -> list[ValidationIssue]:
        if name not in record:
            return []
        value = record[name]
        if not _is_numeric(value):
            return [
                ValidationIssue(
                    row=row,
                    column=name,
                    message=f"column {name!r} has non-numeric value {value!r}",
                )
            ]
        if (lo is not None and value < lo) or (hi is not None and value > hi):
            return [
                ValidationIssue(
                    row=row,
                    column=name,
                    message=(
                        f"column {name!r} has value {value!r} outside range "
                        f"[{lo!r}, {hi!r}]"
                    ),
                )
            ]
        return []

    return check


def one_of(name: str, allowed: Sequence) -> Rule:
    """Build a checker that flags a present value not in *allowed*.

    Only records that *have* the column are inspected; a missing column
    produces no issue here.

    Args:
        name: the column name to check.
        allowed: the sequence of acceptable values.

    Returns:
        A per-record checker. For a record where *name* is present and the
        value is not in *allowed* it yields one issue (``column=name``);
        otherwise no issue.

    Raises:
        ValidationError: if *name* is not a ``str`` or *allowed* is not a
            sequence (a bare ``str``/``bytes`` is rejected).
    """
    _require_str_name(name, "one_of")
    if isinstance(allowed, (str, bytes)) or not isinstance(allowed, Sequence):
        raise ValidationError(
            f"one_of: allowed must be a sequence, got {type(allowed).__name__}"
        )

    def check(record: dict, row: int) -> list[ValidationIssue]:
        if name not in record:
            return []
        value = record[name]
        if value not in allowed:
            return [
                ValidationIssue(
                    row=row,
                    column=name,
                    message=f"column {name!r} has value {value!r} not in {list(allowed)!r}",
                )
            ]
        return []

    return check


class Validator:
    """Run an ordered sequence of per-record checkers over records.

    Args:
        rules: an ordered sequence of per-record checkers (each with the
            signature ``check(record, row) -> list[ValidationIssue]``). Stored
            as a defensive copy; mutating the passed sequence afterwards does
            not affect this validator.

    Example:
        >>> validator = Validator([require_column("id"), in_range("score", 0, 100)])
        >>> validator.validate([{"id": 1, "score": 5}, {"score": 200}])
        [ValidationIssue(row=1, column='id', message="missing required column 'id'"), ...]
    """

    def __init__(self, rules: Sequence[Rule]) -> None:
        # Defensive copy: the caller's sequence is never aliased.
        self._rules = list(rules)

    @property
    def rules(self) -> list[Rule]:
        """The rules, in order (a copy, safe to mutate by the caller)."""
        return list(self._rules)

    def validate(self, records: Sequence[dict]) -> list[ValidationIssue]:
        """Validate a batch of records and return all issues.

        Runs every rule over every record and collects the issues in
        record-major order: all rules for record 0, then all rules for record
        1, and so on.

        Args:
            records: the records to validate (each a ``dict``).

        Returns:
            A new list of :class:`ValidationIssue` in record-major order.
            Empty when no rule finds any issue.
        """
        issues: list[ValidationIssue] = []
        for row, record in enumerate(records):
            for rule in self._rules:
                issues.extend(rule(record, row))
        return issues

    def iter_validate(self, source: Iterator[dict]) -> Iterator[ValidationIssue]:
        """Validate a stream of records lazily, yielding issues one at a time.

        Pulls one record at a time from *source* (never materializing it),
        runs every rule over it, and yields the resulting issues one at a time
        in record-major order. A 0-based row counter tracks each record's
        position in the source.

        Args:
            source: an iterator of records.

        Yields:
            One :class:`ValidationIssue` at a time, in record-major order.
        """
        row = 0
        for record in source:
            for rule in self._rules:
                for issue in rule(record, row):
                    yield issue
            row += 1
