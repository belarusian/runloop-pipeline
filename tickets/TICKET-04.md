# TICKET-04: No tests for CSV ingestion capability

## Title
Test suite only contains an import smoke test; no coverage for reader, schema inference, or error types.

## Evidence
- `tests/` contains only `test_smoke.py` (lines 4-8), which asserts `import pipeline` and `hasattr(pipeline, "__version__")`.
- No `tests/test_ingest.py`, `tests/test_schema.py`, or `tests/test_errors.py`.
- `pyproject.toml` sets `testpaths = ["tests"]` and CI runs `pytest tests/ -x -q`, so the only assertion that ever runs is the import check.

## Impact
The entire CSV ingestion capability (TICKET-01/02/03) would ship with zero behavioral tests. Type-inference edge cases (int vs float vs str, empty columns, mixed types) and error paths are unverified.

## Suggestion
Add tests mirroring the modules:
- `tests/test_ingest.py` — parse a temp CSV, assert coerced values.
- `tests/test_schema.py` — `infer_schema` on sample rows for int/float/str and ambiguous cases.
- `tests/test_errors.py` — assert each error type is raised and is a `PipelineError`.
