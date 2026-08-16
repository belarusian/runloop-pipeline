# TICKET-37: `iter_rows` lacks an optional `schema` parameter to pin column types and skip inference

## Title
`iter_rows` always infers the schema from a bounded sample of leading rows.
There is no way for a caller to supply an explicit `Schema` to pin column
types, mirroring the gap in `read_csv` (TICKET-36) for the streaming path.

## Evidence
- `pipeline/ingest.py:110-113` — `iter_rows` signature:
