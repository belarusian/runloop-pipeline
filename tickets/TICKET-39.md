# TICKET-39: `Pipeline.schema()` always infers; no way to return a pinned schema

## Title
`Pipeline.schema()` unconditionally reads the first source and infers its
schema. Even if `Pipeline` accepted a pinned `schema` (TICKET-38),
`schema()` would still re-read and re-infer, ignoring the pinned schema.
This creates an inconsistency: `run()` would coerce with the pinned types
but `schema()` would report the inferred types.

## Evidence
- `pipeline/pipeline.py:192-204` — `schema()` implementation:
