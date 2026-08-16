# TICKET-17: No `stream_transforms` lazy generator over `Iterator[dict]`

## Title
Cycle 4 requires a `stream_transforms` generator that lazily applies a
sequence of per-record transforms over an `Iterator[dict]` source (e.g.
`iter_rows`). No such generator exists in `pipeline/transform.py`.

## Evidence
- `grep -n "stream_transforms\|def stream" pipeline/transform.py` → no
  matches. The only composition entry point is the batch
  `apply_transforms` (`pipeline/transform.py:261`), which takes and returns a
  `list[dict]`.
- `pipeline/ingest.py:91` — `iter_rows(...) -> Iterator[dict[str, int |
  float | str]]` is the intended streaming source, but nothing in the
  transform phase consumes an `Iterator`.
- `pipeline/transform.py:261` — `apply_transforms(records: list[dict],
  transforms: list[Transform]) -> list[dict]` materializes the whole batch;
  there is no lazy counterpart.

## Impact
- The streaming half of the package's stated purpose (see
  `docs/README.md` "Streaming — planned (Cycle 4)") is unimplemented. A user
  with a large CSV cannot apply per-record transforms without first
  materializing the entire file into a list, defeating the bounded-memory
  design of `iter_rows`.
- The per-record `apply_one` contract (TICKET-14) exists but has no streaming
  consumer, so it is dead surface.

## Suggestion
- Add `stream_transforms(source: Iterator[dict], transforms: list[Transform])
  -> Iterator[dict]` to `pipeline/transform.py`.
  - For each record from *source*, apply each transform's `apply_one` in
    order; if any returns `None`, drop the record and stop for that record.
  - Before iterating, reject any transform with `streamable is False`
    (TICKET-16) by raising `TransformError` naming the op.
  - Be lazy: yield one record at a time; do not materialize the source.
  - Preserve the purity contract: never mutate the source records.
- Export it from `pipeline/__init__.py` (TICKET-19) and document it in
  `docs/API.md`.

---
_GitHub issue: TBD_
