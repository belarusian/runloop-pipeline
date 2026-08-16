# TICKET-2: No batch `run()` entry point

## Title
Pipeline lacks a batch `run()` method

## Evidence
- `pipeline/ingest.py` provides `read_csv()` (returns full list/DataFrame) and `iter_rows()` (generator).
- `pipeline/transform.py` provides transform functions that operate on a collection or a single row.
- No module in `pipeline/` exposes a `run()` method that chains `read_csv` → transforms → final output in one call.
- `grep -rn "def run" pipeline/` returns no results.

## Impact
- Batch workloads (e.g., "load CSV, clean, aggregate, write") require 3+ lines of glue code per pipeline.
- Error handling (e.g., stopping on first bad row) is not centralized.
- No consistent return contract (list vs. DataFrame vs. generator).

## Suggestion
In `pipeline/pipeline.py`:
