# TICKET-03: PipelineError exception hierarchy is missing

## Title
No `PipelineError` base class or `IngestError` / `SchemaError` / `TransformError` subtypes.

## Evidence
- `pipeline/__init__.py` (lines 1-3) defines no exceptions.
- No `pipeline/errors.py` (or equivalent) exists; `ls pipeline/` shows only `__init__.py`.
- `grep -rn "Error\|Exception" pipeline/` returns no matches.

## Impact
Callers cannot catch pipeline failures by a common type. Ingest, schema, and transform failures (TICKET-01, TICKET-02) have no defined error contract, so error handling is ad-hoc and untestable.

## Suggestion
Add `pipeline/errors.py`:
- `class PipelineError(Exception)` as the base.
- `class IngestError(PipelineError)` — file I/O / malformed CSV.
- `class SchemaError(PipelineError)` — schema inference or type mismatch.
- `class TransformError(PipelineError)` — transform-stage failures.
Export all from `pipeline/__init__.py`.
