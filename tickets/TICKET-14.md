# TICKET-14: Per-record (`apply_one`) and purity contract is unspecified and unenforced

## Title
Cycle 4 will wire transforms into a streaming pipeline over `iter_rows`, which
requires every op to be expressible **per record** and to be **pure**
(allocation-based, no mutation). These two contracts are currently unspecified
and have no mechanism to enforce them.

## Evidence
- `pipeline/ingest.py:150-199` — `iter_rows` yields one `record` at a time and
  is the intended streaming source for Cycle 4. There is no per-record transform
  interface to consume it.
- The required ops include `Aggregate(group_by, agg)`, which is inherently
  **batch** (it needs all records to group). The spec says ops must be
  "expressible per-record so Cycle 4 can wire them into a streaming pipeline",
  but it is undefined how a batch-only op like `Aggregate` satisfies that.
- No existing code documents or enforces the "pure, allocation-based (new
  dicts/lists), never mutates input" invariant. `pipeline/schema.py` uses frozen
  dataclasses for value types, but records are plain mutable dicts
  (`pipeline/ingest.py:44`), so nothing prevents an op from mutating them.

## Impact
- Without a defined `apply_one` contract, Cycle 4 has no stable seam to stream
  through: it is unclear whether an op is per-record, batch-only, or both, and
  what `apply_one` returns when a record is dropped (e.g. `Filter`).
- Without an enforced purity invariant, a transform that mutates its input
  records would silently corrupt the shared record objects across a composed
  pipeline and across the batch/streaming paths, and no test would catch it.

## Suggestion
- Define the `Transform` contract explicitly in `pipeline/transform.py`:
  - `apply(records) -> list[record]` (batch) and `apply_one(record) -> record |
    None` (per-record; `None` means "drop this record").
  - State clearly which ops are per-record (`Filter`, `MapColumn`, `Rename`,
    `Select`) and which are batch-only (`Aggregate`), and how a batch-only op
    behaves under `apply_one` (e.g. raise `TransformError` or be marked
    non-streamable) so Cycle 4 can route around it.
- Enforce purity by construction: every op builds new dicts/lists and never
  writes to the input. Add a test that deep-copies a record list, runs a
  transform, and asserts the original is byte-for-byte unchanged.
- Document the contract in `docs/API.md` and `docs/ARCHITECTURE.md`.

---
_GitHub issue: TBD_
