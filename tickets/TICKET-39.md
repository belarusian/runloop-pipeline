# TICKET-39: Pipeline.schema() always infers; no way to return a pinned schema

## Title
Pipeline.schema() unconditionally reads the first source and infers its
schema. Even if Pipeline accepted a pinned schema (TICKET-38), schema()
would still re-read and re-infer, ignoring the pinned schema. This creates
an inconsistency: run() would coerce with the pinned types but schema()
would report the inferred types.

## Evidence
- pipeline/pipeline.py:192-204 — schema() always calls
  read_csv(self._sources[0], self._encoding, self._sample_size) and returns
  the inferred schema. No check for a pre-supplied schema.
- pipeline/pipeline.py:14-15 — the module docstring describes Pipeline.schema
  as "return the inferred Schema of the first source", with no mention of a
  pinned schema.
- pipeline/pipeline.py:213,247 — to_csv and stream_to_csv fall back to
  self.schema() when their schema parameter is None, meaning they would get
  the inferred schema, not the pinned one.

## Impact
- If TICKET-38 is implemented without this fix, Pipeline.schema() would
  return a different schema than the one used for coercion in run()/stream(),
  violating the principle of least surprise.
- to_csv/stream_to_csv would use the inferred schema for output column
  ordering even when the user pinned a different schema at construction.
- Every call to schema() re-reads the file, which is wasteful when a
  pinned schema is available.

## Suggestion
- When a pinned schema is set (TICKET-38), schema() should return it
  directly without reading the file.
- When no pinned schema is set, current behavior (infer from first source)
  is preserved.
- Update the method docstring to document both paths.
- Update the module docstring description of Pipeline.schema.
- Ensure to_csv/stream_to_csv use the pinned schema (via self.schema())
  for output column ordering when no explicit output schema is given.

---
_GitHub issue: TBD_
