# TICKET-38: Pipeline constructor lacks an optional schema parameter to pin ingestion types

## Title
Pipeline.__init__ accepts source, transforms, encoding, and sample_size, but
no schema parameter. The pipeline always infers column types at ingestion
time. The schema parameter on to_csv/stream_to_csv controls output column
ordering only — it does not affect ingestion type coercion.

## Evidence
- pipeline/pipeline.py:78-90 — __init__ signature has no schema parameter.
- pipeline/pipeline.py:165 — run() calls read_csv(src, self._encoding, self._sample_size)
  with no schema.
- pipeline/pipeline.py:185 — stream() calls iter_rows(src, self._encoding,
  self._sample_size) with no schema.
- pipeline/pipeline.py:213,247 — to_csv and stream_to_csv accept
  schema: Schema | None = None, but this is forwarded to write_csv/
  iter_write_csv for output column ordering (pipeline/output.py:35), not
  for ingestion type pinning.

## Impact
- There is no way to pin column types at the pipeline level. A user who
  knows their CSV has a str column that inference would classify as int
  must either pre-process the file or accept the wrong type.
- The existing schema parameter on to_csv/stream_to_csv is misleading:
  a user might expect it to pin ingestion types, but it only controls
  output column order.

## Suggestion
- Add a keyword-only parameter schema: Schema | None = None to
  Pipeline.__init__. Store it as self._schema.
- In run(), pass self._schema to read_csv.
- In stream(), pass self._schema to iter_rows.
- When schema is None (default), current behavior is preserved.
- Document the distinction between the ingestion schema (constructor) and
  the output schema (to_csv/stream_to_csv) in the class docstring.

---
_GitHub issue: https://github.com/belarusian/runloop-pipeline/issues/51_
