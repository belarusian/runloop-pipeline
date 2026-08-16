# TICKET-34: No end-to-end integration test over the full ingest → transform → output → validation loop

## Title
The test suite covers each phase in isolation (ingest, transform, output,
validation) and the `Pipeline` seams (`run`/`stream`, `to_csv`/`stream_to_csv`,
`validate`/`iter_validate`) separately, but there is **no single test** that
drives the full loop — ingest multiple realistic CSVs, apply a transform
chain, write output, and validate the result — on realistic multi-file input.
The phases are never exercised together in one scenario.

## Evidence
- `ls tests/` shows per-phase files (`test_ingest.py`, `test_transform.py`,
  `test_transform_stream.py`, `test_output.py`, `test_validation.py`) and
  per-seam files (`test_pipeline.py`, `test_pipeline_multi.py`,
  `test_pipeline_validate.py`). There is no `test_integration.py` /
  `test_end_to_end.py` / `test_loop.py`.
- `tests/test_pipeline_multi.py` exercises multi-source `run`/`stream`/
  `to_csv`/`stream_to_csv` but never validation; `tests/test_pipeline_validate.py`
  exercises `validate`/`iter_validate` but never `to_csv`
  (`grep -rn "to_csv" tests/test_pipeline_validate.py` returns nothing).
- The multi-source fixtures in the existing tests are minimal (2–4 rows,
  single or two columns, identical headers). No test uses a realistic
  multi-file dataset (e.g. several files with mixed int/float/str columns,
  quoted fields, and a transform that renames/selects/re-aggregates).
- The documented pipeline contract (`docs/ARCHITECTURE.md`, the `Pipeline`
  docstring at `pipeline/pipeline.py:1-40`) describes ingest → transform →
  output (and, per Cycle 7, validation) as one composable flow, but that
  composition is only ever tested phase-by-phase.

## Impact
- Cross-phase interactions are untested: e.g. a transform that reorders or
  drops columns feeding `to_csv`'s schema fallback, a `Filter` that empties the
  stream feeding `stream_to_csv`, or `validate` row indices being global across
  concatenated multi-source input while `to_csv` writes the same records.
- A regression that only manifests when phases are combined (e.g. the
  multi-source column-drop in TICKET-31, or the header-only crash in
  TICKET-32) is not caught by any single existing test.
- Newcomers have no canonical "happy path" example to copy; the README
  (`docs/README.md`) describes the phases but the suite has no test that
  mirrors the full documented flow.

## Suggestion
- Add `tests/test_integration.py` with a realistic multi-file scenario:
  - 2–3 CSV files with mixed column types (int, float, str), a quoted field,
    and a BOM on one file.
  - A transform chain (e.g. `Filter` + `MapColumn` + `Rename` + `Select`).
  - Assert `run()` output, then `to_csv` and `stream_to_csv` produce
    byte-identical files that read back equal to `run()`.
  - Run `validate`/`iter_validate` over the same pipeline and assert the
    issues match expectations (and that batch/streaming validation agree).
- Add a "gate-before-write" scenario: validate first, and only write when the
  issue list is empty (see TICKET-35).
- Keep fixtures small but realistic; reuse a helper to write the CSVs.

---
_GitHub issue: #46
