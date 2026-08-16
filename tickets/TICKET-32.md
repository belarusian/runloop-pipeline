# TICKET-32: Header-only (no data rows) source makes `to_csv`/`stream_to_csv` raise `SchemaError`

## Title
A source CSV that has a header row but **zero** data rows causes
`Pipeline.to_csv` and `Pipeline.stream_to_csv` to raise
`SchemaError("cannot infer schema from an empty sample")` instead of writing an
empty (header-only) output file. The documented empty-input behavior for the
output stage ("empty header when records is empty") is unreachable through the
`Pipeline` seam for this common case.

## Evidence
- `pipeline/pipeline.py:209` `to_csv` calls `self.run()` then
  `self.schema()`; `pipeline/pipeline.py:243` `stream_to_csv` calls
  `self.schema()` up front.
- `pipeline/pipeline.py:192` `schema()` calls `read_csv` on the first source.
- `pipeline/ingest.py` `read_csv` calls `infer_schema(data_rows, ...)` with
  `data_rows == []` for a header-only file.
- `pipeline/schema.py:139` `infer_schema` raises
  `SchemaError("cannot infer schema from an empty sample")` when the sample is
  empty.
- Reproduction:
