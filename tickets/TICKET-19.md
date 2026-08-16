# TICKET-19: Streaming/composition symbols are not exported from `pipeline/__init__.py`

## Title
The Cycle 4 public surface — `stream_transforms`, `compose`, and `Composed` —
is not exported from `pipeline/__init__.py`, so it is not part of the package
API even once implemented.

## Evidence
- `pipeline/__init__.py` imports from `pipeline.transform` only:
  `Aggregate, Filter, MapColumn, Rename, Select, Transform, apply_transforms`.
  There is no import of `stream_transforms`, `compose`, or `Composed`.
- `pipeline/__init__.py` `__all__` lists exactly: `Aggregate, Column, Filter,
  IngestError, MapColumn, PipelineError, Rename, Schema, SchemaError, Select,
  Transform, TransformError, apply_transforms, coerce_value, infer_schema,
  iter_rows, read_csv`. None of the Cycle 4 symbols appear.
- `tests/test_api.py` asserts a fixed expected export set; it does not include
  the Cycle 4 symbols, so a missing export would not be caught.

## Impact
- Even after `stream_transforms`/`compose`/`Composed` are implemented
  (TICKET-17/18), they would be importable only via
  `from pipeline.transform import ...`, not from the package root, breaking
  the convention that the public API is re-exported from `pipeline`.
- `tests/test_api.py` would not detect the omission.

## Suggestion
- Once implemented, add `stream_transforms`, `compose`, and `Composed` to the
  `from pipeline.transform import (...)` block and to `__all__` in
  `pipeline/__init__.py`.
- Update `tests/test_api.py` to include the new symbols in its expected set.
- Update `docs/API.md` and `docs/MODULES.md` to list the new exports.

---
_GitHub issue: TBD_
