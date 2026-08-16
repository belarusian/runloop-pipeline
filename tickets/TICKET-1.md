# TICKET-1: No Pipeline orchestration class exists

## Title
Pipeline class that composes ingest + transforms is missing

## Evidence
- `pipeline/__init__.py` exports `ingest`, `transform`, `schema` submodules but no `Pipeline` class.
- `grep -rn "class Pipeline" pipeline/` returns no results.
- No file `pipeline/pipeline.py` or `pipeline/orchestrator.py` exists.
- The individual stages (`ingest.read_csv`, `ingest.iter_rows`, `transform.*`) exist in isolation but nothing composes them into a single callable pipeline.

## Impact
- Callers must manually wire ingest → transform → transform → … in application code.
- No single entry point for batch (`run()`) or streaming (`stream()`) execution.
- Schema propagation across stages is unenforced; a transform can silently drop or rename columns without a `schema()` accessor to validate.
- Testing a pipeline end-to-end requires re-implementing the composition each time.

## Suggestion
Create `pipeline/pipeline.py` with:
