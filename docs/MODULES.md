# Module catalog

Catalog of every module in the `pipeline` package, what it does, and how it
relates to the others.

## pipeline/

### `pipeline/__init__.py`
Package root. Re-exports the public API and defines `__all__` and
`__version__`.

- **Exports:** `Column`, `Schema`, `infer_schema`, `coerce_value`,
  `read_csv`, `iter_rows`, `PipelineError`, `IngestError`, `SchemaError`,
  `TransformError`, `Transform`, `Filter`, `MapColumn`, `Rename`, `Select`,
  `Aggregate`, `apply_transforms`.
- **Depends on:** `errors`, `ingest`, `schema`, `transform`.
- **Note:** the Cycle 4 streaming/composition symbols (`stream_transforms`,
  `compose`, `Composed`) are **not yet exported** — see `TICKET-19`.

### `pipeline/errors.py`
Exception hierarchy. No dependencies.

- `PipelineError(Exception)` — base for all pipeline failures.
- `IngestError(PipelineError)` — file/CSV problems.
- `SchemaError(PipelineError)` — inference / coercion problems.
- `TransformError(PipelineError)` — transform-stage failures.

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
- **Used by:** `ingest`.

### `pipeline/ingest.py`
CSV ingestion.

- `read_csv(path, encoding="utf-8-sig", sample_size=1000) -> (Schema, list[record])`.
- `iter_rows(path, encoding="utf-8-sig", sample_size=1000) -> Iterator[record]`
  — the lazy streaming source intended for the Cycle 4 streaming path.
- **Depends on:** `errors` (`IngestError`), `schema` (`Schema`, `coerce_value`,
  `infer_schema`).
- **Produces:** the `list[record]` (batch) or `Iterator[record]` (streaming)
  that the Transformation phase consumes.

### `pipeline/transform.py`
Transformation phase. Implemented (Cycle 3).

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
- **Depends on:** `errors` (`TransformError`).
- **Consumes:** `list[record]` from `ingest`.
- **Produces:** `list[record]` for downstream phases.
- **Cycle 4 target (not yet implemented):** a `streamable` class attribute, a
  lazy `stream_transforms(source: Iterator[dict], transforms) ->
  Iterator[dict]` generator, and a `compose()` helper returning a `Composed`
  Transform. See [API.md](API.md#streaming--composition-cycle-4-target) and
  tickets `TICKET-16` … `TICKET-20`.

## Dependency graph

    errors ──┬── schema ── ingest ──┐
             └── transform ─────────┴── __init__
