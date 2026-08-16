# TICKET-22: No `OutputError` in the exception hierarchy for the output stage

## Title
The exception hierarchy has no error type for the (missing) output/write stage,
so a future CSV writer cannot honor the "never a bare `Exception`" contract.

## Evidence
- `pipeline/errors.py` defines exactly four classes:
  `PipelineError` (line 11), `IngestError` (line 15), `SchemaError` (line 19),
  `TransformError` (line 23).
- `grep -rn "OutputError\|WriteError" pipeline/` returns no matches.
- `pipeline/__init__.py` exports only `IngestError`, `PipelineError`,
  `SchemaError`, `TransformError` (see `__all__`).
- The module docstrings state the failure contract explicitly:
  `pipeline/ingest.py:1-11` ("File I/O problems ... raise `IngestError`"),
  `pipeline/transform.py:1-22` ("every failure in this module raises
  `TransformError`, never a bare `Exception`"), and `pipeline/pipeline.py:1-24`
  ("this module never raises a bare `Exception`").
- A CSV writer (see `TICKET-21`) would raise `OSError` (non-writable path) or
  `UnicodeEncodeError` (undecodable output) — neither is a `PipelineError`
  subtype, so the writer would either leak a bare exception or be forced to
  mislabel write failures as `IngestError` (which is documented as a
  *read/parse* problem).

## Impact
- The output stage (TICKET-21) cannot be added without either breaking the
  documented "never a bare `Exception`" contract or polluting `IngestError`
  with write failures, which is semantically wrong and breaks callers that
  catch `IngestError` to handle read problems.
- Callers cannot distinguish "failed to read the source" from "failed to write
  the output" with a single `except PipelineError` and a narrow subtype.

## Suggestion
- Add to `pipeline/errors.py`:
  - `class OutputError(PipelineError)` — "Raised when the output stage cannot
    write the transformed records (e.g. non-writable path, undecodable output)."
- Export `OutputError` from `pipeline/__init__.py` and add it to `__all__`.
- Document it in `docs/API.md` under the Errors table and in the output-stage
  section (see `TICKET-25`).
- Add a test asserting `OutputError` is a `PipelineError` subtype and that a
  write failure (e.g. a directory path or read-only location) raises
  `OutputError`, not a bare `OSError`.

---
_GitHub issue: https://github.com/belarusian/runloop-pipeline/issues/32_
