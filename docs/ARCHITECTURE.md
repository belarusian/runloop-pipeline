# Architecture

## The phase model

The pipeline is a sequence of **phases**. Each phase is a pure function over
records: it takes records in, produces records out, and never mutates its
input. This makes phases composable and independently testable.

## Batch vs. streaming

Each phase is pure over records. The Transformation phase supports two shapes:

- **Batch** — `apply_transforms(records, transforms)` over a `list[record]`.
  Every op participates via `apply`.
- **Streaming** (Cycle 4 target) — `stream_transforms(source, transforms)`
  over an `Iterator[record]` (e.g. `iter_rows`), applying each op's
  `apply_one` lazily, one record at a time. Only per-record ops are
  streamable; the `streamable` class attribute marks which ops may appear in a
  streaming pipeline, so batch-only ops (e.g. `Aggregate`) are rejected up
  front rather than failing mid-stream.

`compose()` folds a sequence of transforms into a single `Composed` Transform
so a pipeline can be treated as one first-class op and reused across both the
batch and streaming paths. See `TICKET-16` … `TICKET-20`.
