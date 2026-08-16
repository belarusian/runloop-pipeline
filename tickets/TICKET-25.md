# TICKET-25: Docs describe only ingest + transform; the output stage is absent

## Title
`docs/MODULES.md`, `docs/ARCHITECTURE.md`, and `docs/API.md` describe the
pipeline as ingest + transform only. There is no documentation of an output
stage (CSV writer) or of multi-source composition, so the documented API does
not match the intended end-to-end pipeline.

## Evidence
- `docs/MODULES.md` — the `pipeline/` catalog lists `errors`, `schema`,
  `ingest`, `transform`, and `__init__`. The dependency graph at the bottom is
  `errors ── schema ── ingest ── transform ── __init__`. There is no output
  module and no output node in the graph.
- `docs/ARCHITECTURE.md` — the "phase model" describes phases as "takes records
  in, produces records out" and the "Batch vs. streaming" section covers only
  the Transformation phase (`apply_transforms` / `stream_transforms`). No
  output/write phase is described.
- `docs/API.md` — sections are Errors, Schema, Ingestion, Transformation, and
  "Streaming + Composition (Cycle 4 target)". There is no Output/Writer
  section and no `to_csv`/`stream_to_csv` entry.
- `pipeline/__init__.py` `__all__` exports no writer symbol, consistent with
  the code (TICKET-21) but leaving the docs with no place to document one.
- The `Pipeline` docstring (`pipeline/pipeline.py:1-24`) and the
  `pipeline/__init__.py` package docstring ("ingesting and transforming CSV
  datasets") both frame the package as read+transform only.

## Impact
- A newcomer landing at the repo (per `docs/README.md`) reads the docs and
  concludes the pipeline cannot write output, and cannot find any guidance on
  how to persist transformed records or combine multiple sources.
- When the output stage (TICKET-21) and multi-source support (TICKET-23,
  TICKET-24) are implemented, the docs will be stale unless updated in the
  same change, risking a divergence between code and documentation.
- The dependency graph in `MODULES.md` will be wrong (missing the output node)
  until it is updated.

## Suggestion
- Update the docs to reflect the full ingest → transform → output pipeline:
  - `docs/MODULES.md`: add an output-stage entry (or note the writer lives in
    `pipeline/pipeline.py`), and add the output node to the dependency graph.
  - `docs/ARCHITECTURE.md`: add an "Output phase" subsection describing
    `to_csv` (batch) and `stream_to_csv` (streaming) and their failure
    contract (`OutputError`, TICKET-22).
  - `docs/API.md`: add an "Output" section documenting `Pipeline.to_csv` and
    `Pipeline.stream_to_csv`, and a "Multi-source" section documenting the
    `source: str | Sequence[str]` contract and the schema reconciliation rule
    (TICKET-24).
- Update the `Pipeline` and package docstrings to mention the output stage and
  multi-source support once implemented.
- Keep the docs in the same commit as the corresponding code change so they
  never diverge.

---
_GitHub issue: https://github.com/belarusian/runloop-pipeline/issues/35_
