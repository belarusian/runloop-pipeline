# TICKET-3: No streaming `stream()` entry point over `iter_rows`

## Title
Pipeline lacks a `stream()` method that composes `iter_rows` with transforms

## Evidence
- `pipeline/ingest.py` defines `iter_rows(path)` as a generator yielding one row (dict) at a time.
- `pipeline/transform.py` transforms are written to accept either a full list or a single row (inconsistent — see code).
- No `stream()` method exists anywhere in `pipeline/`.
- `grep -rn "def stream" pipeline/` returns no results.

## Impact
- Large files cannot be processed without materializing the entire CSV in memory.
- The streaming path (generator → generator → generator) is the natural fit for `iter_rows` but is unimplemented.
- Memory usage scales with file size instead of staying O(1) per row.

## Suggestion
In `pipeline/pipeline.py`:
