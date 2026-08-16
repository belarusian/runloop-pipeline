# API reference

Public interface of the `pipeline` package. Everything listed here is exported
from `pipeline/__init__.py` (see `pipeline.__all__`).

## Errors

| Name | Base | Purpose |
|------|------|---------|
| `PipelineError` | `Exception` | Base for all pipeline failures. |
| `IngestError` | `PipelineError` | File unreadable / undecodable / malformed CSV. |
| `SchemaError` | `PipelineError` | Inference failed / value not coercible. |
| `TransformError` | `PipelineError` | A transform op failed (Cycle 3). |

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

## Transformation phase (Cycle 3 target)

`pipeline/transform.py` is **not yet implemented**. The intended public
interface, to be added and exported from `pipeline/__init__.py`:

### `Transform` (protocol / ABC)
The base contract every op implements.

- `apply(records: list[record]) -> list[record]`
  Batch entry point. Pure: returns a new list, never mutates the input.
- `apply_one(record: record) -> record | None`
  Per-record variant. Return the transformed record, or `None` to drop it
  (used by `Filter`). Ops that are not naturally per-record (e.g. `Aggregate`)
  may implement `apply` only and raise `TransformError` from `apply_one`, or
  document that they are batch-only.

### Concrete ops
- `Filter(predicate: Callable[[record], bool])` — keep records where
  `predicate(record)` is truthy. `apply_one` returns the record or `None`.
- `MapColumn(name: str, fn: Callable[[int | float | str], int | float | str])`
  — replace `record[name]` with `fn(record[name])` in a new dict.
- `Rename(old: str, new: str)` — rename a column in a new dict.
- `Select(names: list[str])` — keep only the named columns, in order.
- `Aggregate(group_by: list[str], agg: dict[str, Callable])` — one output row
  per distinct `group_by` key; `agg` maps a column to an aggregation function
  (e.g. `sum`, `len`). Batch-only.

### Composition
- `apply_transforms(records: list[record], transforms: list[Transform]) -> list[record]`
  Apply each transform in order. Pure: returns a new list; the input list and
  its records are never mutated.

### Failure contract
Every failure in this module raises `TransformError` (from
`pipeline/errors.py`), never a bare `Exception`. Examples: an unknown column in
`MapColumn`/`Rename`/`Select`, a `predicate`/`fn` that raises, or an
`Aggregate` over a non-numeric column.

> Status: **TBD** — see tickets `TICKET-11` … `TICKET-15`.
