# TICKET-5: Transform interface is inconsistent (batch vs. row)

## Title
Transform functions in `pipeline/transform.py` have mixed signatures, blocking uniform composition

## Evidence
- `pipeline/transform.py` contains transform functions where some accept `list[dict]` (batch) and others accept a single `dict` (row).
- There is no common protocol or base class (e.g., `Transform` ABC with `apply_batch` / `apply_row`).
- `pipeline/__init__.py` exports the module but does not define or export a `Transform` type.
- Without a uniform interface, a `Pipeline` cannot iterate `self._transforms` and call a single method.

## Impact
- A `Pipeline.run()` (batch) and `Pipeline.stream()` (row) cannot share the same transform list without type-checking at call time.
- Adding a new transform requires the author to guess which signature the pipeline expects.
- Static type checkers (mypy) cannot verify transform compatibility.

## Suggestion
Define a protocol in `pipeline/transform.py`:
