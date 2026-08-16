# TICKET-4: No `schema()` accessor for output validation

## Title
Pipeline lacks a `schema()` accessor to declare/validate output columns

## Evidence
- `pipeline/schema.py` exists and defines schema-related utilities (column definitions, validation helpers).
- No `Pipeline.schema()` method exists to return the expected output schema after all transforms.
- Transforms in `pipeline/transform.py` can add, remove, or rename columns; nothing enforces that the final output matches a declared schema.
- `grep -rn "def schema" pipeline/` returns no results in any class context.

## Impact
- Downstream consumers (writers, APIs, tests) cannot introspect what columns a pipeline will produce.
- A transform that accidentally drops a column fails silently at runtime rather than at composition time.
- Schema drift between stages is undetectable without manual inspection.

## Suggestion
In `pipeline/pipeline.py`:
