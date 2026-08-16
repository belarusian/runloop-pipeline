# Architecture

## The phase model

The pipeline is a sequence of **phases**. Each phase is a pure function over
records: it takes records in, produces records out, and never mutates its
input. This makes phases composable and independently testable. The phases are
**Ingest** (CSV → records), **Transform** (records → records), and **Output**
(records → CSV).

## Batch vs. streaming

Each phase is pure over records. The Transformation and Output phases each
support two shapes:

- **Batch** — `apply_transforms(records, transforms)` over a `list[record]`,
  and `write_csv(records, path)` over a `list[record]`. Every op participates
  via `apply`; the writer materializes the whole record list.
- **Streaming** — `stream_transforms(source, transforms)` and
  `iter_write_csv(source, path)` over an `Iterator[record]` (e.g.
  `iter_rows`), applying each op's `apply_one` lazily and writing each row as
  it is pulled, one record at a time. Only per-record ops are streamable; the
  `streamable` class attribute marks which ops may appear in a streaming
  pipeline, so batch-only ops (e.g. `Aggregate`) are rejected up front rather
  than failing mid-stream.

`compose()` folds a sequence of transforms into a single `Composed` Transform
so a pipeline can be treated as one first-class op and reused across both the
batch and streaming paths.

## Multi-source

`Pipeline` accepts a single `str` path or a `Sequence[str]` of paths. A bare
`str` is normalized to a one-element list; a sequence is stored as a defensive
copy. `run()` reads each source in order and concatenates the records;
`stream()` chains `iter_rows` over each source in order (never materializing
any source); `schema()` returns the schema of the **first** source. A
single-source pipeline behaves exactly as before.

## Output phase

The Output phase persists transformed records back to a CSV file. It is the
write counterpart of the Ingest phase and is implemented in
`pipeline/output.py`.

- **Batch** — `Pipeline.to_csv(path, *, schema=None, encoding="utf-8")` runs
  the pipeline (`run()`) and writes the records via `write_csv`. When `schema`
  is `None` the pipeline's `schema()` (the first source's schema) fixes the
  column order.
- **Streaming** — `Pipeline.stream_to_csv(path, *, schema=None,
  encoding="utf-8")` pipes `stream()` through `iter_write_csv`, writing each
  record one at a time without full materialization. It never calls `run()`.

Column order is taken from an explicit `Schema` when supplied, else derived
from the records (first-seen union for the batch writer, the first record's
keys for the streaming writer). Values render via `str()`; a column missing
from a record renders as an empty string.

**Failure contract:** every output failure raises `OutputError` (from
`pipeline/errors.py`), never a bare `Exception`. This keeps write failures
(e.g. `OSError` on a non-writable path, `UnicodeEncodeError` on output)
distinguishable from read failures (`IngestError`) and transform failures
(`TransformError`) under a single `except PipelineError`.
