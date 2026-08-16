# TICKET-21: Pipeline has no output stage (no CSV writer)

## Title
The `Pipeline` class reads a CSV source and transforms it, but has no output
stage: there is no way to persist the transformed records back to a CSV file.

## Evidence
- `pipeline/pipeline.py` exposes exactly three entry points:
  `run()` (line 92), `stream()` (line 109), and `schema()` (line 130).
  `run()` returns `list[dict[str, int | float | str]]` (line 92); `stream()`
  returns an `Iterator[dict[str, int | float | str]]` (line 109). Neither
  writes anywhere.
- `grep -rn "to_csv\|write_csv\|stream_to_csv\|csv.writer\|csv.DictWriter"
  pipeline/` returns no matches in any `.py` file (only a stale `.pyc`).
- `pipeline/ingest.py` is read-only: `read_csv` (line 33) and `iter_rows`
  (line 91) open files in `"r"` mode only. There is no symmetric write path.
- The module docstring (`pipeline/pipeline.py:1-24`) describes the pipeline as
  "ingest a source, apply transforms" — no output phase is mentioned.

## Impact
- The pipeline is read-only end to end: a caller that wants "load CSV, clean,
  write CSV" must hand-roll `csv.DictWriter` glue outside the package,
  duplicating the encoding/`utf-8-sig`/BOM handling that `ingest` already
  centralizes.
- There is no streaming write path, so large transformed datasets cannot be
  written incrementally; the caller must materialize the full list first.
- The "never a bare `Exception`" failure contract (see `pipeline/errors.py`)
  has no home for write failures (e.g. `OSError` on a non-writable path,
  `UnicodeEncodeError` on output), so a future writer would either leak bare
  exceptions or be forced to misuse `IngestError`.

## Suggestion
- Add an output stage to `pipeline/pipeline.py`:
  - `Pipeline.to_csv(path, encoding="utf-8-sig") -> None` — batch: run the
    pipeline and write the records to *path* with a header derived from the
    first record's keys (or the inferred schema).
  - `Pipeline.stream_to_csv(path, encoding="utf-8-sig") -> None` — streaming:
    pipe `stream()` through a `csv.DictWriter` so records are written one at a
    time without full materialization.
- Derive the header from the pipeline's schema (see `schema()`, line 130) so
  column order is stable and matches the inferred schema.
- Route all write failures through a dedicated `OutputError` (see
  `TICKET-22`), never a bare `Exception`.
- Add tests: round-trip (write then `read_csv` equals the transformed records),
  streaming write equals batch write, and a non-writable path raising
  `OutputError`.

---
_GitHub issue: TBD_
