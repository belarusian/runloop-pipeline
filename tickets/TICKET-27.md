# TICKET-27: No `ValidationError` in the exception hierarchy

## Title
`pipeline/errors.py` has no `ValidationError`. The hierarchy defines one
subclass per existing stage (`IngestError`, `SchemaError`, `TransformError`,
`OutputError`), but the validation stage (TICKET-26) has no corresponding
error type, so validation failures cannot be caught distinctly under
`PipelineError`.

## Evidence
- `pipeline/errors.py` (29 lines) defines exactly five classes:
  `PipelineError` (line 11), `IngestError` (line 15), `SchemaError` (line 19),
  `TransformError` (line 23), `OutputError` (line 27). There is no
  `ValidationError`.
- The module docstring (`pipeline/errors.py:1-7`) states "Every failure raised
  by the pipeline derives from `PipelineError`" and the per-stage convention is
  one subclass per stage. The validation stage is missing from that set.
- `grep -rn "ValidationError" pipeline/ tests/` returns no matches.
- `tests/test_errors.py` parametrizes over
  `[IngestError, SchemaError, TransformError, OutputError]` (and the base) for
  the "is a `PipelineError`" / "is raisable" / "catchable by base type" tests;
  a `ValidationError` would be added to these lists.

## Impact
- When the validation stage (TICKET-26) is implemented, its failures have no
  home: raising a bare `Exception` breaks the documented failure contract
  ("never a bare `Exception`"), and reusing an existing type (e.g.
  `SchemaError`) would conflate a *validation* failure (a record violates a
  business rule) with a *schema* failure (a cell cannot be coerced to its
  inferred type) — two different failure classes that callers may want to
  handle differently.
- Callers cannot write `except ValidationError:` to isolate data-quality
  failures from ingest/transform/output failures, defeating the single-base
  `except PipelineError` design.

## Suggestion
- Add to `pipeline/errors.py`:
