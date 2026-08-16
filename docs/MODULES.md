# Module catalog

Catalog of every module in the `pipeline` package, what it does, and how it
relates to the others.

## pipeline/

### `pipeline/__init__.py`
Package root. Re-exports the public API and defines `__all__` and
`__version__`.

- **Exports:** `Column`, `Schema`, `infer_schema`, `coerce_value`,
  `read_csv`, `iter_rows`, `PipelineError`, `IngestError`, `SchemaError`,
  `TransformError`, `OutputError`, `Transform`, `Filter`, `MapColumn`,
  `Rename`, `Select`, `Aggregate`, `apply_transforms`, `stream_transforms`,
  `compose`, `Composed`, `Pipeline`, `write_csv`, `iter_write_csv`.
- **Depends on:** `errors`, `ingest`, `schema`, `transform`, `output`,
  `pipeline`.

### `pipeline/errors.py`
Exception hierarchy. No dependencies.

- `PipelineError(Exception)` — base for all pipeline failures.
- `IngestError(PipelineError)` — file/CSV problems.
- `SchemaError(PipelineError)` — inference / coercion problems.
- `TransformError(PipelineError)` — transform-stage failures.
- `OutputError(PipelineError)` — output/write-stage failures (non-writable
  path, undecodable output).

### `pipeline/schema.py`
Schema model and type inference.

- `Column(name, type)` — frozen dataclass, a named column + Python type.
- `Schema(columns)` — frozen dataclass, ordered collection of `Column`.
  Methods: `names()`, `column(name)`, `types()`, `to_dict()`, `project(names)`,
  `__len__`, `__iter__`.
- `infer_schema(sample_rows, header=None, sample_size=None) -> Schema` —
  classify each column as `int` / `float` / `str`.
- `coerce_value(value, col_type) -> int | float | str` — coerce one string cell.
- **Depends on:** `errors` (`SchemaError`).
- **Used by:** `ingest`, `output`.

### `pipeline/ingest.py`
CSV ingestion.

- `read_csv(path, encoding="utf-8-sig", sample_size=1000) -> (Schema, list[record])`.
- `iter_rows(path, encoding="utf-8-sig", sample_size=1000) -> Iterator[record]`
  — the lazy streaming source used by the streaming path.
- **Depends on:** `errors` (`IngestError`), `schema` (`Schema`, `coerce_value`,
  `infer_schema`).
- **Produces:** the `list[record]` (batch) or `Iterator[record]` (streaming)
  that the Transformation phase consumes.

### `pipeline/transform.py`
Transformation phase.

- `Transform` (ABC) — base contract: `apply_one(record) -> record | None`
  (per-record; `None` drops the record) and `apply(records) -> list[record]`
  (default maps `apply_one` over the batch).
- `Filter(predicate)` — keep records where `predicate(record)` is truthy.
- `MapColumn(name, fn)` — replace `record[name]` with `fn(record[name])`.
- `Rename(old, new)` — rename a column.
- `Select(names)` — keep only the named columns, in order.
- `Aggregate(group_by, agg)` — batch-only; one row per distinct `group_by`
  key. `apply_one` raises `TransformError`.
- `apply_transforms(records, transforms) -> list[record]` — batch composition.
- `stream_transforms(source, transforms) -> Iterator[record]` — lazy
  streaming composition over an `Iterator[record]`.
- `Composed(transforms)` — a `Transform` wrapping a tuple of transforms.
- `compose(*transforms) -> Composed` — fold a sequence into one `Composed`.
- **Depends on:** `errors` (`TransformError`).
- **Consumes:** `list[record]` (batch) or `Iterator[record]` (streaming)
  from `ingest`.
- **Produces:** `list[record]` / `Iterator[record]` for the output phase.

### `pipeline/output.py`
Output stage (Cycle 6). The write counterpart of `ingest`.

- `write_csv(records, path, *, schema=None, encoding="utf-8") -> int` — batch
  writer: writes a header row plus one row per record, returns the number of
  data rows written.
- `iter_write_csv(source, path, *, schema=None, encoding="utf-8") ->
  Iterator[int]` — lazy streaming writer (a generator): consumes an
  `Iterator[record]` one record at a time, writing each row and yielding a
  running row count.
- Column order comes from an explicit `Schema` when given, else from the
  records (first-seen union for `write_csv`, first record's keys for
  `iter_write_csv`). Values render via `str()`; missing keys render as an
  empty string.
- **Depends on:** `errors` (`OutputError`), `schema` (`Schema`).
- **Consumes:** `list[record]` (batch) or `Iterator[record]` (streaming)
  from `transform`.
- **Failure contract:** every failure raises `OutputError`, never a bare
  `Exception`.

### `pipeline/pipeline.py`
Orchestration (Cycle 5, extended in Cycle 6).

- `Pipeline(source, transforms=(), *, encoding="utf-8-sig", sample_size=1000)`
  — `source` is a `str` or a `Sequence[str]` (multi-source). A bare `str` is
  normalized to a one-element list; a sequence is stored as a defensive copy.
- `run() -> list[record]` — batch: read every source in order, concatenate,
  apply transforms.
- `stream() -> Iterator[record]` — streaming: lazily chain `iter_rows` over
  each source in order, piped through `stream_transforms`.
- `schema() -> Schema` — the inferred schema of the **first** source.
- `to_csv(path, *, schema=None, encoding="utf-8") -> int` — batch output via
  `write_csv`.
- `stream_to_csv(path, *, schema=None, encoding="utf-8") -> int` — streaming
  output via `iter_write_csv` (never calls `run`).
- **Depends on:** `errors` (`PipelineError`), `ingest`, `schema`, `transform`,
  `output`.

## Dependency graph

    errors ──┬── schema ──┬── ingest ────────────┐
             │            └── output ────────────┤
             └── transform ──────────────────────┼── pipeline ── __init__
                                                 │
    ingest (source) → transform (records) → output (CSV)
