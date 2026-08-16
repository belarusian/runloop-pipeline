# TICKET-12: `TransformError` is declared and exported but never raised

## Title
`TransformError` is part of the public API but no code path ever raises it, so
the "transform failures raise `TransformError`" contract is unenforceable and
untested.

## Evidence
- `pipeline/errors.py:23-24` — `class TransformError(PipelineError):` with
  docstring "Raised when a transform-stage operation fails."
- `pipeline/__init__.py:3` — `from pipeline.errors import ... TransformError`.
- `pipeline/__init__.py:15` — `"TransformError"` is listed in `__all__`.
- `grep -rn "raise TransformError" . --include="*.py"` → **no matches**. The
  exception is never raised anywhere in the package.
- `tests/test_errors.py:9,15,23` only assert that `TransformError` is a
  `PipelineError` subclass and is raisable by hand; no test exercises a real
  transform failure.

## Impact
- The error hierarchy advertises a transform-stage failure type that no code
  produces. A caller catching `TransformError` to handle transform failures
  would never see it, because there is no transform stage to fail.
- Once the transform module lands (TICKET-11), the contract "transform
  failures raise `TransformError`, never a bare `Exception`" has no existing
  precedent or tests to anchor it.

## Suggestion
- When implementing `pipeline/transform.py` (TICKET-11), wrap every op failure
  (unknown column, predicate/fn raising, bad aggregation) in `TransformError`,
  chaining the original exception with `from exc`.
- Add tests in `tests/test_transform.py` asserting that each failure mode
  raises `TransformError` (and therefore `PipelineError`).

---
_GitHub issue: TBD_
