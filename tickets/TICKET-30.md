# TICKET-30: Docs and tests omit the validation stage entirely

## Title
The documentation (`docs/README.md`, `docs/ARCHITECTURE.md`,
`docs/MODULES.md`, `docs/API.md`) and the test suite have no trace of a
validation stage. When the validation stage (TICKET-26), `ValidationError`
(TICKET-27), `ValidationIssue` + rule factories (TICKET-28), and the
`Pipeline.validate()`/`iter_validate()` seam (TICKET-29) are implemented, the
docs and tests must be updated in the same change or they will diverge.

## Evidence
- `docs/README.md` phase table lists exactly four rows — Ingestion,
  Transformation, Output, Orchestration — all marked "implemented". There is
  no Validation row.
- `docs/ARCHITECTURE.md` describes the phase model as Ingest → Transform →
  Output and has a "Batch vs. streaming" section that pairs
  `apply_transforms`/`stream_transforms` and `write_csv`/`iter_write_csv`;
  there is no `validate`/`iter_validate` pair and no "Validation phase"
  subsection.
- `docs/MODULES.md` catalogs `errors`, `schema`, `ingest`, `transform`,
  `output`, `pipeline` and the dependency graph. There is no `validate` node,
  no `ValidationError` in the `errors.py` entry, and no `ValidationIssue` /
  rule-factory entries.
- `docs/API.md` has Errors, Schema, Ingestion, Transformation, Output, and
  Pipeline sections. The Errors table lists five classes (no
  `ValidationError`); there is no Validation section and no
  `Pipeline.validate`/`iter_validate` entry.
- `tests/` has `test_errors.py`, `test_schema.py`, `test_ingest.py`,
  `test_transform.py`, `test_transform_stream.py`, `test_output.py`,
  `test_pipeline.py`, `test_pipeline_multi.py`, `test_api.py`. There is no
  `test_validate.py` and no validation cases in `test_errors.py` /
  `test_api.py`.

## Impact
- A newcomer landing at the repo (per `docs/README.md`) reads the docs and
  concludes the pipeline cannot validate data quality, and cannot find any
  guidance on how to check records against column-level rules.
- When the validation stage is implemented, stale docs will describe a
  three-phase pipeline while the code has four, and the dependency graph in
  `MODULES.md` will be wrong (missing the `validate` node and the
  `ValidationError` edge).
- Without a `test_validate.py`, the batch/streaming equivalence that the rest
  of the suite checks (e.g. `test_transform_stream.py` asserts
  `stream_transforms` over `iter_rows` equals `apply_transforms` over
  `read_csv`) is unverified for validation: `iter_validate` over `iter_rows`
  should yield the same issues as `validate` over `read_csv`.

## Suggestion
- Update the docs in the same commit as the validation code:
  - `docs/README.md`: add a Validation row to the phase table (module
    `pipeline/validate.py`, In → Out: `list[record]` / `Iterator[record]` →
    `list[ValidationIssue]` / `Iterator[ValidationIssue]`).
  - `docs/ARCHITECTURE.md`: add a "Validation phase" subsection describing
    `validate` (batch) and `iter_validate` (streaming), the reporting (not
    filtering) contract, and the `ValidationError` failure contract.
  - `docs/MODULES.md`: add a `pipeline/validate.py` entry, add
    `ValidationError` to the `errors.py` entry, and add the `validate` node to
    the dependency graph.
  - `docs/API.md`: add `ValidationError` to the Errors table, add a
    "Validation phase" section documenting `Validator`, `ValidationIssue`, the
    four rule factories, and `Pipeline.validate`/`iter_validate`.
- Add `tests/test_validate.py` covering: each rule factory (pass and fail),
  a record with multiple violations yielding multiple issues, an empty/valid
  input yielding `[]`, `ValidationError` for an unevaluatable rule, and the
  batch/streaming equivalence (`iter_validate` over `iter_rows` == `validate`
  over `read_csv`).
- Extend `tests/test_errors.py` (TICKET-27) and `tests/test_api.py` to cover
  the new exports.

---
_GitHub issue: #41
