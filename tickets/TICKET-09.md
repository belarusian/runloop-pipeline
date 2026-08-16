# TICKET-09: Schema inference scans all rows; no bounded sample_size

## Title
`read_csv` passes the full data-row list to `infer_schema`, and `infer_schema`
has no `sample_size` bound, so type inference cost grows with file size.

## Evidence
- `pipeline/ingest.py:54` — `schema = infer_schema(data_rows, header=header)`
  passes every data row (not a sample).
- `pipeline/schema.py:68` — `def infer_schema(sample_rows: list[list[str]],
  header: list[str] | None = None) -> Schema:` has no `sample_size` parameter.
- `pipeline/schema.py:89` — `width = max(len(row) for row in sample_rows)`
  iterates every row.
- `pipeline/schema.py:92` — `non_empty = [row[index] for row in sample_rows if
  index < len(row) and row[index] != ""]` builds a per-column list from every
  row, so inference is O(rows * columns).
- `grep -n "sample_size" pipeline/schema.py pipeline/ingest.py` returns no
  matches.

## Impact
- For large files, schema inference re-reads and re-parses the entire dataset
  just to classify column types, duplicating the work of the record loop and
  defeating any streaming path (TICKET-08).
- A single late-occurring outlier (e.g. one non-numeric cell in an otherwise
  numeric column) flips the whole column to `str` based on a value that may be
  an anomaly; a bounded sample would make inference both cheaper and more
  predictable.

## Suggestion
- Add a `sample_size: int | None = None` parameter to `infer_schema`
  (`pipeline/schema.py:68`) and slice `sample_rows[:sample_size]` before
  computing `width` (line 89) and the per-column `non_empty` lists (line 92).
- Thread a `sample_size` argument through `read_csv` (`pipeline/ingest.py:54`)
  so callers can bound inference, defaulting to a sensible value (e.g. 1000).
- Add tests: inference with `sample_size` smaller than the row count uses only
  the first N rows; `sample_size=None` preserves current behavior.
