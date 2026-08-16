# TICKET-29: No `Pipeline.validate()` / `Pipeline.iter_validate()` seam

## Title
`Pipeline` exposes `run`/`stream` (ingest + transform) and `to_csv`/
`stream_to_csv` (output), but no `validate()` / `iter_validate()` seam to run
the validation stage (TICKET-26) over the pipeline's records.

## Evidence
- `pipeline/pipeline.py` defines exactly these public methods: `run` (line
  146), `stream` (line 167), `schema` (line 191), `to_csv` (line 208),
  `stream_to_csv` (line 242). There is no `validate` or `iter_validate`.
- The class already owns the source normalization (`_normalize_sources`, line
  94), the encoding, the sample size, and the transform list — everything a
  validation seam needs to reuse. The batch/streaming pattern is established:
  `run` reads via `read_csv` and `stream` chains `iter_rows` (line 185); a
  validation seam would follow the same shape.
- `grep -rn "def validate\|def iter_validate" pipeline/` returns no matches.
- The `Pipeline` docstring (`pipeline/pipeline.py:1-40`) enumerates the entry
  points (`run`, `stream`, `schema`, `to_csv`, `stream_to_csv`) and omits any
  validation entry point.

## Impact
- A caller who has a `Pipeline` cannot validate its output without
  materializing `run()` and hand-rolling a `Validator` call, or re-reading the
  sources separately. The validation stage is not reachable through the
  orchestrator, so it is not part of the documented pipeline contract.
- The streaming symmetry is broken at the orchestrator level: `stream()`
  exists for bounded-memory execution, but there is no `iter_validate()` to
  validate a stream lazily. A caller validating a large multi-source pipeline
  must fully materialize `run()`.
- The phase model (`docs/ARCHITECTURE.md`) and the phase table
  (`docs/README.md`) describe the pipeline as ingest → transform → output;
  without a `Pipeline.validate()` seam the validation stage (TICKET-26) has no
  first-class place in the orchestration.

## Suggestion
- Add to `Pipeline`:
  - `validate(rules: Sequence[Rule]) -> list[ValidationIssue]` — batch: run
    the pipeline (`run()`) and pass the records through
    `Validator(rules).validate(...)`, returning all issues.
  - `iter_validate(rules: Sequence[Rule]) -> Iterator[ValidationIssue]` —
    streaming: pipe `stream()` through `Validator(rules).iter_validate(...)`,
    yielding issues one at a time without materializing any source (mirrors
    `stream_to_csv`, which pipes `stream()` through `iter_write_csv` without
    calling `run`).
- Accept the rules as a parameter (so a `Pipeline` can be validated against
  different rule sets) or as a constructor argument with a default of `()`;
  document whichever is chosen. A parameter keeps `Pipeline`'s constructor
  signature stable and matches how `to_csv`/`stream_to_csv` take their
  `schema`/`encoding` as call arguments.
- Update the `Pipeline` docstring and the `Raises:` sections to mention
  `ValidationError` (TICKET-27) for the validation seam.
- Keep the seam thin: it should delegate to `Validator` (TICKET-26) and not
  re-implement rule evaluation.

---
_GitHub issue: #40
