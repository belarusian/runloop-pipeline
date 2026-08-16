# API reference

Public interface of the `pipeline` package. Everything listed here is exported
from `pipeline/__init__.py` (see `pipeline.__all__`).

## Errors

| Name | Base | Purpose |
|------|------|---------|
| `PipelineError` | `Exception` | Base for all pipeline failures. |
| `IngestError` | `PipelineError` | File unreadable / undecodable / malformed CSV. |
| `SchemaError` | `PipelineError` | Inference failed / value not coercible. |
| `TransformError` | `PipelineError` | A transform op failed. |

## Schema

- `Column(name: str, type: type)` — frozen dataclass.
- `Schema(columns: tuple[Column, ...])` — frozen dataclass.
  - `names() -> list[str]`
  - `column(name: str) -> Column` — raises `SchemaError` if absent.
  - `types() -> dict[str, type]`
  - `to_dict() -> dict[str, type]` — alias for `types()`.
  - `project(names: list[str]) -> Schema` — new schema with only those columns;
    raises `SchemaError` for an unknown name.
  - `__len__`, `__iter__`.
- `infer_schema(sample_rows, header=None, sample_size=None) -> Schema`
- `coerce_value(value: str, col_type: type) -> int | float | str`

## Ingestion

- `read_csv(path, encoding="utf-8-sig", sample_size=1000) -> (Schema, list[record])`
- `iter_rows(path, encoding="utf-8-sig", sample_size=1000) -> Iterator[record]`

where `record = dict[str, int | float | str]`.

## Transformation phase (implemented)

`pipeline/transform.py` is implemented. All symbols below are exported from
`pipeline/__init__.py`.

### `Transform` (ABC)
The base contract every op implements.

- `apply_one(record: record) -> record | None`
  Per-record variant. Return the transformed record, or `None` to drop it
  (used by `Filter`). Batch-only ops raise `TransformError` from `apply_one`.
- `apply(records: list[record]) -> list[record]`
  Batch entry point. The default maps `apply_one` over *records* and drops
  `None` results. Pure: returns a new list, never mutates the input.

### Concrete ops
- `Filter(predicate: Callable[[record], bool])` — keep records where
  `predicate(record)` is truthy. `apply_one` returns the record or `None`.
- `MapColumn(name: str, fn: Callable[[int | float | str], int | float | str])`
  — replace `record[name]` with `fn(record[name])` in a new dict.
- `Rename(old: str, new: str)` — rename a column in a new dict.
- `Select(names: list[str])` — keep only the named columns, in order.
- `Aggregate(group_by: list[str], agg: dict[str, str])` — one output row per
  distinct `group_by` key (first-seen order); `agg` maps a column to a kind in
  `{'sum','mean','count','min','max'}`. Batch-only: `apply_one` raises
  `TransformError`.

### Composition
- `apply_transforms(records: list[record], transforms: list[Transform]) -> list[record]`
  Apply each transform in order. Pure: returns a new list; the input list and
  its records are never mutated.

### Failure contract
Every failure in this module raises `TransformError` (from
`pipeline/errors.py`), never a bare `Exception`. Examples: an unknown column in
`MapColumn`/`Rename`/`Select`, a `predicate`/`fn` that raises, or an
`Aggregate` over a non-numeric column.

## Streaming + Composition (Cycle 4 target)

Not yet implemented. The intended additions to `pipeline/transform.py`, to be
exported from `pipeline/__init__.py`:

- `Transform.streamable: bool` — class attribute, `True` for per-record ops
  (`Filter`, `MapColumn`, `Rename`, `Select`), `False` for batch-only ops
  (`Aggregate`). Lets a streaming path detect and reject non-streamable ops
  up front. See `TICKET-16`.
- `stream_transforms(source: Iterator[record], transforms: list[Transform]) -> Iterator[record]`
  Lazily apply a sequence of per-record transforms over an `Iterator[record]`
  source (e.g. `iter_rows`). Yields one record at a time; drops a record if any
  op's `apply_one` returns `None`; rejects any non-streamable op up front by
  raising `TransformError`. See `TICKET-17`.
- `Composed(Transform)` — a `Transform` wrapping a sequence of transforms;
  `apply_one`/`apply` chain the members, and `streamable` is `True` only if
  every member is streamable.
- `compose(*transforms: Transform) -> Composed` — fold a sequence into a single
  `Composed`. See `TICKET-18`.

> Status: **TBD** — see tickets `TICKET-16` … `TICKET-20`.
