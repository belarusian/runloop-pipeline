# TICKET-40: No validation that a pinned schema's column names match the CSV header

## Title
When a caller supplies an explicit `Schema` to `read_csv`/`iter_rows`
(TICKET-36/37), there is no validation that the schema's column names match
the CSV header. `_coerce_row` zips `schema.columns` with the row **by
position**, so a mismatch in column order or set will silently misalign
values or truncate columns.

## Evidence
- `pipeline/ingest.py:25-31` — `_coerce_row`:
