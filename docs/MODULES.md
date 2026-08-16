# Module catalog

Catalog of every module in the `pipeline` package, what it does, and how it
relates to the others.

## pipeline/

### `pipeline/__init__.py`
Package root. Re-exports the public API and defines `__all__` and
`__version__`.

- **Exports:** `Column`, `Schema`, `infer_schema`, `coerce_value`,
  `read_csv`, `iter_rows`, `PipelineError`, `IngestError`, `SchemaError`,
  `TransformError`.
- **Depends on:** `errors`, `ingest`, `schema`.
- **Note:** `TransformError` is exported but not yet raised by any module —
  the transform phase (Cycle 3) is what will use it. Transform ops are not yet
  exported (module TBD).

### `pipeline/errors.py`
Exception hierarchy. No dependencies.

- `PipelineError(Exception)` — base for all pipeline failures.
- `IngestError(PipelineError)` — file/CSV problems.
- `SchemaError(PipelineError)` — inference / coercion problems.
- `TransformError(PipelineError)` — transform-stage failures (Cycle 3).

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
- `iter_rows(path, encoding="utf-8-sig", sample_size=1000) -> Iterator[record]`.
- **Depends on:** `errors` (`IngestError`), `schema` (`Schema`, `coerce_value`,
  `infer_schema`).
- **Produces:** the `list[record]` that the Transformation phase consumes.

### `pipeline/transform.py` — **TBD (Cycle 3 target)**
Does not exist yet. Intended to provide the `Transform` protocol/ABC, the
concrete ops (`Filter`, `MapColumn`, `Rename`, `Select`, `Aggregate`), and
`apply_transforms`. See [API.md](API.md#transformation-phase-cycle-3-target)
and tickets `TICKET-11` … `TICKET-15`.

- **Depends on (planned):** `errors` (`TransformError`).
- **Consumes:** `list[record]` from `ingest`.
- **Produces:** `list[record]` for downstream phases.

## Dependency graph
