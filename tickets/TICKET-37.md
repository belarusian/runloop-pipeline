# TICKET-37: iter_rows lacks an optional schema parameter to pin column types and skip inference

## Title
iter_rows always infers the schema from a bounded sample of leading rows.
There is no way for a caller to supply an explicit Schema to pin column
types, mirroring the gap in read_csv (TICKET-36) for the streaming path.

## Evidence
- pipeline/ingest.py:110-113 — iter_rows signature has no schema parameter.
- pipeline/ingest.py:149-166 — the code reads up to sample_size rows into a
  sample list, then calls infer_schema(sample, header=header, sample_size=sample_size).
  This sample-reading phase is purely for inference.
- pipeline/ingest.py:168-175 — after inference, every row is coerced via
  _coerce_row(row, schema).

## Impact
- Same type-pinning gap as TICKET-36, but on the streaming path.
- Additionally, when a schema is provided, the current design would still
  read and buffer sample_size rows before yielding the first record,
  adding unnecessary latency and memory for no benefit.

## Suggestion
- Add a keyword-only parameter schema: Schema | None = None to iter_rows.
- When schema is provided: skip the sample-reading phase entirely, validate
  that schema.names() == header (see TICKET-40), coerce every row directly
  against the provided schema as it is read. The sample_size parameter is
  ignored (or documented as ignored).
- When schema is None (default), current behavior is preserved.
- Update the module docstring and the iter_rows docstring.

---
_GitHub issue: https://github.com/belarusian/runloop-pipeline/issues/50_
