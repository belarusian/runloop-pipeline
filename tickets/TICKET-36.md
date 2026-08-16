# TICKET-36: read_csv lacks an optional schema parameter to pin column types and skip inference

## Title
read_csv always infers the schema from data rows. There is no way for a
caller to supply an explicit Schema to pin column types, which is needed
when inference is wrong or undesirable (e.g. a ZIP-code column that must stay
str, or a numeric column that should be float but the sample is all-integer).

## Evidence
- pipeline/ingest.py:48-50 — read_csv signature has no schema parameter.
- pipeline/ingest.py:102 — the schema is always inferred via infer_schema.
- pipeline/schema.py:148-175 — infer_schema classifies columns as int, float,
  or str based on whether all non-empty cells parse. A column of ZIP codes
  like "00501", "01453" will be inferred as int (all parse), losing leading
  zeros on coercion.

## Impact
- Callers cannot override inferred types. Any column whose semantic type
  differs from the inferred type (leading-zero strings, fixed-width numeric
  fields, columns that should be float but the sample is all-integer) will
  be silently coerced to the wrong type.
- The only workaround is to pre-process the CSV externally, which defeats the
  purpose of the pipeline.

## Suggestion
- Add a keyword-only parameter schema: Schema | None = None to read_csv.
- When schema is provided: skip the infer_schema call, validate that
  schema.names() == header (see TICKET-40), use the provided schema for
  _coerce_row, and return the provided schema as the first tuple element.
- When schema is None (default), current behavior is preserved.
- Update the module docstring and the read_csv docstring.

---
_GitHub issue: https://github.com/belarusian/runloop-pipeline/issues/49_
