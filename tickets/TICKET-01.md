# TICKET-01: CSV reader module is missing

## Title
No CSV reader exists to open a file, parse rows, and coerce values.

## Evidence
- `pipeline/__init__.py` (lines 1-3) contains only a docstring and `__version__`. There is no `pipeline/ingest.py` (or equivalent) module.
- `ls pipeline/` shows only `__init__.py` and `__pycache__` — no reader module.
- `tests/test_smoke.py` (lines 4-8) only asserts the package imports and exposes `__version__`; nothing exercises CSV parsing.

## Impact
The stated capability — "a CSV reader that opens a file, parses rows, infers column schema, and coerces values" — is entirely unimplemented. No code path can ingest a CSV file.

## Suggestion
Add `pipeline/ingest.py` with a `read_csv(path)` (or `Ingestor`) that:
- opens the file,
- parses header + rows,
- infers per-column schema (see TICKET-02),
- coerces each cell to its inferred type,
- raises `IngestError` (see TICKET-03) on I/O or malformed input.
Export the public entry point from `pipeline/__init__.py`.
