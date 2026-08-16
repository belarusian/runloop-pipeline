# TICKET-36: `read_csv` lacks an optional `schema` parameter to pin column types and skip inference

## Title
`read_csv` always infers the schema from data rows. There is no way for a
caller to supply an explicit `Schema` to pin column types, which is needed
when inference is wrong or undesirable (e.g. a ZIP-code column that must stay
`str`, or a numeric column that should be `float` but the sample is all-integer).

## Evidence
- `pipeline/ingest.py:48-50` — `read_csv` signature:
