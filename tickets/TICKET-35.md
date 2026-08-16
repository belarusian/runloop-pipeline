# TICKET-35: No `validate` → `to_csv` gate-before-write workflow or test

## Title
`Pipeline.validate`/`iter_validate` (Cycle 7) and `Pipeline.to_csv`/
`stream_to_csv` (Cycle 6) are independent seams. There is no first-class
"validate, then write only if clean" workflow, and no test that exercises the
common gate-before-write pattern: run validation, and write the output file
only when the issue list is empty.

## Evidence
- `pipeline/pipeline.py:282` `validate` and `pipeline/pipeline.py:309`
  `iter_validate` return/yield `ValidationIssue`s; `pipeline/pipeline.py:209`
  `to_csv` and `pipeline/pipeline.py:243` `stream_to_csv` write the file. The
  two are never connected in the code.
- `grep -rn "to_csv\|stream_to_csv" tests/test_pipeline_validate.py` returns
  nothing — the validation tests never write output, and the output tests
  (`tests/test_pipeline_multi.py`) never validate.
- There is no `Pipeline` method (e.g. `validate_and_write`, or a
  `to_csv(..., rules=...)` that refuses to write on issues) and no documented
  recipe in `docs/` for gating a write on a clean validation.
- The `Pipeline` docstring (`pipeline/pipeline.py:1-40`) lists `run`, `stream`,
  `schema`, `to_csv`, `stream_to_csv` (and, per Cycle 7, `validate`/
  `iter_validate`) as independent entry points; it does not describe composing
  them into a gate.

## Impact
- A caller who wants "don't ship a file with bad rows" must hand-roll the
  gate: call `validate(rules)`, check `if not issues`, then call `to_csv`.
  Nothing in the API or docs standardizes this, so it is easy to get wrong
  (e.g. writing before checking, or checking a different rule set than the
  one the data was produced under).
- The streaming path is awkward to gate: `iter_validate` is lazy, so a caller
  must drain it to know whether to write, which means either materializing the
  issues or re-running the pipeline for the write. There is no documented
  pattern for this.
- No test pins the gate semantics (write on clean, skip on dirty), so a
  future API change that couples validation and output would be unverified.

## Suggestion
- Decide whether to add a first-class gate or document the manual pattern:
  - Option A (documented recipe): add a section to `docs/ARCHITECTURE.md` and
    the `Pipeline` docstring showing the canonical gate:
    `issues = pipeline.validate(rules); if not issues: pipeline.to_csv(path)`.
  - Option B (API): add `Pipeline.validate_and_write(path, rules, *,
    schema=None, encoding="utf-8") -> int` that runs `validate` and writes via
    `to_csv` only when the issue list is empty (returning the row count, or
    raising a new error / returning 0 on a dirty result — document the choice).
- Add a test in `tests/test_integration.py` (or `test_pipeline_validate.py`):
  a pipeline whose data passes the rules writes the file and returns the row
  count; a pipeline whose data fails the rules does **not** write the file
  (assert the path does not exist, or the method reports the issues).
- If Option B is chosen, keep the seam thin (delegate to `validate` +
  `to_csv`) and update `docs/API.md` and `docs/README.md` accordingly.

---
_GitHub issue: TBD
