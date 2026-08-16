# TICKET-08: read_csv materializes the entire file in memory (no streaming)

## Title
`read_csv` reads every row into a list and returns a fully materialized list of
records; there is no lazy/streaming row generator for large files.

## Evidence
- `pipeline/ingest.py:35` — `rows = list(csv.reader(handle))` materializes the
  whole file into a list of lists before any processing.
- `pipeline/ingest.py:56-61` — the record loop appends every coerced record to
  `records: list[dict[...]]`, and the function signature
  (`pipeline/ingest.py:20`) returns `tuple[Schema, list[dict[...]]]`.
- `grep -n "yield\|generator" pipeline/ingest.py` returns no matches — there is
  no generator or iterator path.

## Impact
- Peak memory is proportional to the entire dataset (raw rows + coerced
  records), so ingesting a multi-GB CSV can exhaust memory or swap.
- Callers cannot process rows incrementally (e.g. filter, write, or aggregate
  on the fly) because the full result must be built before anything is
  returned.

## Suggestion
- Add a streaming entry point, e.g. `iter_csv(path, encoding="utf-8-sig") ->
  Iterator[dict]` that yields one coerced record at a time, or a
  `read_csv(..., stream: bool = False)` mode.
- Keep schema inference bounded (see TICKET-09) so the streaming path does not
  require the full row list.
- Add a test asserting the generator yields records lazily (e.g. the first
  record is available without the file being fully consumed).
