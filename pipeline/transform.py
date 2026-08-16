"""Transformation phase for the pipeline package.

A :class:`Transform` is a pure, allocation-based operation over records
(``dict[str, int | float | str]``). Every op either maps records one at a time
via :meth:`Transform.apply_one` (per-record ops: :class:`Filter`,
:class:`MapColumn`, :class:`Rename`, :class:`Select`) or operates on the whole
batch via :meth:`Transform.apply` (batch-only op: :class:`Aggregate`).

The per-record contract: :meth:`apply_one` returns the transformed record, or
``None`` to drop the record. The default :meth:`apply` maps :meth:`apply_one`
over the input and drops ``None`` results, so per-record ops get a batch entry
point for free.

Purity: every op builds new dicts/lists and never writes to the input list or
its record dicts. Composition is provided by :func:`apply_transforms`, which
applies a sequence of transforms in order and returns a new list.

Failure contract: every failure in this module raises
:class:`~pipeline.errors.TransformError`, never a bare ``Exception``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator

from pipeline.errors import TransformError

# A single aggregation kind accepted by :class:`Aggregate`.
_NUMERIC_KINDS = frozenset({"sum", "mean", "min", "max"})
_VALID_KINDS = frozenset({"sum", "mean", "count", "min", "max"})


class Transform(ABC):
    """Base contract for a transform op.

    Per-record ops implement :meth:`apply_one`; the default :meth:`apply` maps
    :meth:`apply_one` over the records and drops any ``None`` results. Batch-only
    ops (e.g. :class:`Aggregate`) override :meth:`apply` and raise
    :class:`~pipeline.errors.TransformError` from :meth:`apply_one`.

    The :attr:`streamable` class attribute marks whether an op can run in a
    streaming (per-record, bounded-memory) pipeline. Per-record ops are
    streamable by default; batch-only ops set it to ``False`` so that
    :func:`stream_transforms` and :func:`compose` reject them up front.
    """

    #: Whether this op can participate in a streaming pipeline.
    streamable: bool = True

    @abstractmethod
    def apply_one(self, record: dict) -> dict | None:
        """Transform a single record.

        Returns:
            The transformed record, or ``None`` to drop the record.
        """

    def apply(self, records: list[dict]) -> list[dict]:
        """Apply this transform to a batch of records.

        The default implementation maps :meth:`apply_one` over *records* and
        drops any ``None`` results. Pure: returns a new list and never mutates
        the input list or its record dicts.
        """
        result: list[dict] = []
        for record in records:
            transformed = self.apply_one(record)
            if transformed is not None:
                result.append(transformed)
        return result


class Filter(Transform):
    """Keep records for which *predicate* is truthy.

    Args:
        predicate: a callable taking a record and returning a truthy value to
            keep the record or a falsy value to drop it.
    """

    def __init__(self, predicate: Callable[[dict], bool]) -> None:
        self._predicate = predicate

    def apply_one(self, record: dict) -> dict | None:
        """Return *record* if the predicate keeps it, else ``None``.

        Raises:
            TransformError: if the predicate raises.
        """
        try:
            keep = self._predicate(record)
        except Exception as exc:
            raise TransformError(f"Filter predicate raised: {exc}") from exc
        return record if keep else None


class MapColumn(Transform):
    """Replace ``record[name]`` with ``fn(record[name])`` in a new dict.

    Args:
        name: the column to rewrite.
        fn: a callable mapping the old value to the new value.
    """

    def __init__(self, name: str, fn: Callable[[int | float | str], int | float | str]) -> None:
        self._name = name
        self._fn = fn

    def apply_one(self, record: dict) -> dict | None:
        """Return a new dict with ``record[name]`` replaced by ``fn(...)``.

        Raises:
            TransformError: if *name* is not in *record*, or if *fn* raises.
        """
        if self._name not in record:
            raise TransformError(f"MapColumn: column {self._name!r} not in record")
        try:
            new_value = self._fn(record[self._name])
        except Exception as exc:
            raise TransformError(f"MapColumn fn raised for column {self._name!r}: {exc}") from exc
        new_record = dict(record)
        new_record[self._name] = new_value
        return new_record


class Rename(Transform):
    """Rename a column in a new dict.

    Args:
        old: the existing column name.
        new: the replacement column name.
    """

    def __init__(self, old: str, new: str) -> None:
        self._old = old
        self._new = new

    def apply_one(self, record: dict) -> dict | None:
        """Return a new dict with key *old* renamed to *new*.

        Raises:
            TransformError: if *old* is not in *record*.
        """
        if self._old not in record:
            raise TransformError(f"Rename: column {self._old!r} not in record")
        new_record: dict = {}
        for key, value in record.items():
            new_record[self._new if key == self._old else key] = value
        return new_record


class Select(Transform):
    """Keep only the named columns, in the given order.

    Args:
        names: the column names to keep, in the desired output order.
    """

    def __init__(self, names: list[str]) -> None:
        self._names = list(names)

    def apply_one(self, record: dict) -> dict | None:
        """Return a new dict holding only the named columns.

        Raises:
            TransformError: if any name is not in *record*.
        """
        for name in self._names:
            if name not in record:
                raise TransformError(f"Select: column {name!r} not in record")
        return {name: record[name] for name in self._names}


class Aggregate(Transform):
    """Group records and aggregate columns within each group (batch-only).

    Args:
        group_by: the column names to group by.
        agg: a mapping of column name to an aggregation kind, one of
            ``'sum'``, ``'mean'``, ``'count'``, ``'min'``, or ``'max'``.

    :meth:`apply` produces one output row per distinct ``group_by`` key, in
    first-seen order. Each output row is
    ``{**{g: record[g] for g in group_by}, **{col: result for col, kind in
    agg.items()}}``. ``'count'`` is ``len(values)``; ``'sum'``/``'mean'``/
    ``'min'``/``'max'`` require every value to be an ``int`` or ``float``
    (not ``bool``, not ``str``). This op is batch-only: :meth:`apply_one`
    raises :class:`~pipeline.errors.TransformError`.
    """

    #: Aggregation needs the whole batch, so it cannot stream.
    streamable: bool = False

    def __init__(self, group_by: list[str], agg: dict[str, str]) -> None:
        self._group_by = list(group_by)
        self._agg = dict(agg)
        for kind in self._agg.values():
            if kind not in _VALID_KINDS:
                raise TransformError(f"Aggregate: unknown aggregation kind {kind!r}")

    def apply_one(self, record: dict) -> dict | None:
        """Always raise: aggregation needs the whole batch.

        Raises:
            TransformError: always, because this op is batch-only.
        """
        raise TransformError("Aggregate is batch-only and cannot be applied per-record")

    def apply(self, records: list[dict]) -> list[dict]:
        """Group *records* and aggregate each group into one output row.

        Raises:
            TransformError: if a ``group_by`` or ``agg`` column is missing from
                a record, or if a numeric aggregation is applied to a
                non-numeric value.
        """
        groups: dict[tuple, list[dict]] = {}
        order: list[tuple] = []
        for record in records:
            for group_col in self._group_by:
                if group_col not in record:
                    raise TransformError(
                        f"Aggregate: group_by column {group_col!r} not in record"
                    )
            key = tuple(record[group_col] for group_col in self._group_by)
            if key not in groups:
                groups[key] = []
                order.append(key)
            groups[key].append(record)

        output: list[dict] = []
        for key in order:
            group_records = groups[key]
            row: dict = {group_col: key[index] for index, group_col in enumerate(self._group_by)}
            for col, kind in self._agg.items():
                for record in group_records:
                    if col not in record:
                        raise TransformError(f"Aggregate: agg column {col!r} not in record")
                values = [record[col] for record in group_records]
                row[col] = self._aggregate(values, kind)
            output.append(row)
        return output

    @staticmethod
    def _aggregate(values: list[int | float | str], kind: str) -> int | float:
        """Compute a single aggregation over *values* for *kind*.

        Raises:
            TransformError: if a numeric kind is applied to a non-numeric value.
        """
        if kind == "count":
            return len(values)
        numeric: list[int | float] = []
        for value in values:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TransformError(
                    f"Aggregate: {kind!r} requires numeric values, got {value!r}"
                )
            numeric.append(value)
        if kind == "sum":
            return sum(numeric)
        if kind == "mean":
            return sum(numeric) / len(numeric)
        if kind == "min":
            return min(numeric)
        if kind == "max":
            return max(numeric)
        raise TransformError(f"Aggregate: unknown aggregation kind {kind!r}")


def apply_transforms(records: list[dict], transforms: list[Transform]) -> list[dict]:
    """Apply a sequence of transforms to *records* in order.

    Pure: returns a new list; the input list and its record dicts are never
    mutated.

    Args:
        records: the input records.
        transforms: the transforms to apply, in order.

    Returns:
        The transformed records as a new list.
    """
    current = list(records)
    for transform in transforms:
        current = transform.apply(current)
    return current


def stream_transforms(
    source: Iterator[dict],
    transforms: list[Transform],
) -> Iterator[dict]:
    """Lazily apply a sequence of streamable transforms to a record stream.

    This is the streaming analogue of :func:`apply_transforms`. It does not
    materialize *source*: it pulls one record at a time, applies each
    transform's :meth:`Transform.apply_one` in order, and yields the result.
    A record is dropped (not yielded) if any step returns ``None``. Peak
    memory is bounded by a single record plus the transform list.

    Args:
        source: an iterator yielding input records.
        transforms: the transforms to apply, in order. Every transform must
            have :attr:`Transform.streamable` set to ``True``.

    Yields:
        One transformed record per surviving input record.

    Raises:
        TransformError: if any transform in *transforms* is not streamable
            (e.g. a batch-only :class:`Aggregate`).
    """
    for transform in transforms:
        if not transform.streamable:
            raise TransformError(
                f"stream_transforms: transform {type(transform).__name__!r} is not "
                "streamable and cannot be used in a streaming pipeline"
            )

    for record in source:
        current: dict | None = record
        for transform in transforms:
            if current is None:
                break
            current = transform.apply_one(current)
        if current is not None:
            yield current


class Composed(Transform):
    """A single transform that chains a tuple of transforms in order.

    :meth:`apply_one` runs each member's :meth:`Transform.apply_one` in order,
    threading the intermediate record through. If any member returns ``None``,
    the chain stops and ``None`` is returned (the record is dropped). The
    batch entry point :meth:`apply` is inherited from :class:`Transform`, so it
    maps :meth:`apply_one` over a list and drops ``None`` results.

    A :class:`Composed` is streamable only if every member is streamable; this
    is enforced at construction by :func:`compose`.

    Args:
        transforms: the member transforms, in the order they should run.
    """

    def __init__(self, transforms: tuple[Transform, ...]) -> None:
        self._transforms = tuple(transforms)

    @property
    def transforms(self) -> tuple[Transform, ...]:
        """The member transforms, in order."""
        return self._transforms

    def apply_one(self, record: dict) -> dict | None:
        """Chain each member's :meth:`apply_one`, dropping on the first ``None``.

        Returns:
            The fully transformed record, or ``None`` if any member dropped it.
        """
        current: dict | None = record
        for transform in self._transforms:
            if current is None:
                break
            current = transform.apply_one(current)
        return current


def compose(*transforms: Transform) -> Composed:
    """Build a :class:`Composed` transform from *transforms*, in order.

    Args:
        *transforms: the transforms to chain, in the order they should run.

    Returns:
        A :class:`Composed` whose :meth:`apply_one` runs the members in order.

    Raises:
        TransformError: if any argument is not streamable (e.g. a batch-only
            :class:`Aggregate`).
    """
    for transform in transforms:
        if not transform.streamable:
            raise TransformError(
                f"compose: transform {type(transform).__name__!r} is not streamable "
                "and cannot be composed into a streaming transform"
            )
    return Composed(tuple(transforms))
