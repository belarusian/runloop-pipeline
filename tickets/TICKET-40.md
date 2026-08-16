# TICKET-40: No validation that a pinned schema column names match the CSV header

## Title
When a caller supplies an explicit Schema to read_csv/iter_rows
(TICKET-36/37), there is no validation that the schema column names match
the CSV header. _coerce_row zips schema.columns with the row by position,
so a mismatch in column order or set will silently misalign values or
truncate columns.

## Evidence
- pipeline/ingest.py:25-31 — _coerce_row uses zip(schema.columns, row) to
  pair columns and cells by position, not by name. If the schema has columns
  [b, a] but the CSV header is [a, b], the value in column a will be coerced
  as if it were column b.
- pipeline/ingest.py:88-95 — read_csv validates row width against the header
  (len(row) != width), but does not validate the schema against the header.
- pipeline/schema.py:148-175 — infer_schema builds columns from the header,
  so inferred schemas always match. A user-supplied schema has no such
  guarantee.
- pipeline/schema.py:44-52 — Schema.column(name) raises SchemaError for a
  missing name, but _coerce_row never calls it.

## Impact
- A schema with columns in a different order than the CSV header will
  silently coerce values into the wrong columns. No error is raised.
- A schema with an extra column not in the header will be silently truncated
  by zip (the extra column is never populated).
- A schema missing a column that is in the header will also be silently
  truncated (the extra header column is never read).
- This is a data-integrity hazard: the output records will have the wrong
  values under the wrong keys, and no error will be raised.

## Suggestion
- When a schema is provided to read_csv/iter_rows, validate that
  schema.names() == header (exact match in name and order).
- On mismatch, raise SchemaError with a clear message showing the expected
  (header) vs actual (schema) column names.
- This validation should happen once at the start of read_csv/iter_rows,
  before any rows are processed.
- Add tests for: matching schema (pass), reordered columns (fail), extra
  column (fail), missing column (fail).

---
_GitHub issue: TBD_
