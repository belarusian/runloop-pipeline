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
| `OutputError` | `PipelineError` | Output stage could not write records (non-writable path, undecodable output). |

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

## Transformation phase

`pipeline/transform.py`. All symbols below are exported from
`pipeline/__init__.py`.

### `Transform` (ABC)
The base contract every op implements.

- `apply_one(record: record) -> record | None`
  Per-record variant. Return the transformed record, or `None` to drop it
  (used by `Filter`). Batch-only ops raise `TransformError` from `apply_one`.
- `apply(records: list[record]) -> list[record]`
  Batch entry point. The default maps `apply_one` over *records* and drops
  `None` results. Pure: returns a new list, never mutates the input.
- `streamable: bool` — class attribute; `True` for per-record ops, `False`
  for batch-only ops.

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
- `stream_transforms(source: Iterator[record], transforms: list[Transform]) -> Iterator[record]`
  Lazily apply a sequence of streamable transforms over an `Iterator[record]`
  source. Yields one record at a time; drops a record if any op's `apply_one`
  returns `None`; rejects any non-streamable op up front by raising
  `TransformError`.
- `Composed(transforms: tuple[Transform, ...])` — a `Transform` wrapping a
  sequence of transforms; `apply_one`/`apply` chain the members.
- `compose(*transforms: Transform) -> Composed` — fold a sequence into a single
  `Composed`.

### Failure contract
Every failure in this module raises `TransformError` (from
`pipeline/errors.py`), never a bare `Exception`.

## Output phase

`pipeline/output.py` is the write counterpart of the Ingest phase. All symbols
below are exported from `pipeline/__init__.py`.

- `write_csv(records: Sequence[record], path: str, *, schema: Schema | None = None, encoding: str = "utf-8") -> int`
  Batch writer. Opens *path* for writing, writes a header row then one row per
  record, and returns the number of data rows written. Column order is
  `schema.names()` when *schema* is given, else the union of the records' keys
  in first-seen order (an empty header when *records* is empty). Each value
  renders via `str()`; a missing key renders as an empty string.
- `iter_write_csv(source: Iterator[record], path: str, *, schema: Schema | None = None, encoding: str = "utf-8") -> Iterator[int]`
  Lazy streaming writer (a generator). Opens *path*, writes the header
  (`schema.names()` if given, else the first record's keys, else an empty
  header), then consumes *source* one record at a time, writing each row and
  yielding a running row count (`1, 2, 3, ...`). The source is never fully
  materialized.

### Pipeline output entry points
- `Pipeline.to_csv(path: str, *, schema: Schema | None = None, encoding: str = "utf-8") -> int`
  Batch output: run the pipeline and write the records via `write_csv`. When
  *schema* is `None` the pipeline's `schema()` (first source) fixes the column
  order. Returns the number of data rows written.
- `Pipeline.stream_to_csv(path: str, *, schema: Schema | None = None, encoding: str = "utf-8") -> int`
  Streaming output: pipe `stream()` through `iter_write_csv`, writing records
  one at a time without full materialization. Never calls `run()`. When
  *schema* is `None` the pipeline's `schema()` (first source) fixes the column
  order. Returns the final running count.

### Failure contract
Every output failure raises `OutputError` (from `pipeline/errors.py`), never a
bare `Exception`. This keeps write failures (e.g. `OSError` on a non-writable
path, `UnicodeEncodeError` on output) distinguishable from read failures
(`IngestError`) and transform failures (`TransformError`).

## Orchestration

- `Pipeline(source: str | Sequence[str], transforms: Sequence[Transform] = (), *, encoding: str = "utf-8-sig", sample_size: int = 1000)`
  Composes the ingest, transform, and output stages. See the Multi-source
  section below for the `source` contract.
  - `run() -> list[record]` — batch: read every source in order, concatenate,
    apply transforms.
  - `stream() -> Iterator[record]` — streaming: lazily chain `iter_rows` over
    each source in order, piped through `stream_transforms`.
  - `schema() -> Schema` — the inferred schema of the **first** source.
  - `to_csv(...) -> int` / `stream_to_csv(...) -> int` — see the Output phase.
  - `source` property — the original `str` when there is one source, else a
    defensive copy of the source list.
  - `sources` property — the normalized source paths, in order (a copy).

## Multi-source

`Pipeline` accepts a single `str` path or a `Sequence[str]` of paths.

- A bare `str` is normalized to a one-element list internally; a sequence is
  stored as a **defensive copy** (mutating the caller's list afterwards does
  not affect the pipeline). An empty sequence raises `PipelineError`.
- `run()` reads each source in order via `read_csv` and concatenates the
  records in source order before applying the transforms.
- `stream()` chains `iter_rows` over each source in order (never materializing
  any source) and pipes the result through `stream_transforms`.
- `schema()` returns the schema of the **first** source only; later sources
  are not inspected. This is the documented reconciliation rule for
  multi-source pipelines: the first source's header/types define the schema
  used for output column order.
- A single-source pipeline (a bare `str`, or a one-element sequence) behaves
  exactly as before: `source` returns the original `str`, and `run`/`stream`/
  `schema` operate on that one file.
