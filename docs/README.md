# runloop-pipeline — documentation

A small, typed data pipeline for ingesting, transforming, and writing CSV
datasets. Records are plain `dict[str, int | float | str]` values; the pipeline
is organized as a sequence of **phases** that each consume and produce records.

## Phases

| Phase | Status | Module | In → Out |
|-------|--------|--------|----------|
| Ingestion | implemented | `pipeline/ingest.py` | CSV file → `Schema` + `list[record]` (or a lazy iterator) |
| Transformation | implemented | `pipeline/transform.py` | `list[record]` / `Iterator[record]` → same |
| Output | implemented | `pipeline/output.py` | `list[record]` / `Iterator[record]` → CSV file |
| Orchestration | implemented | `pipeline/pipeline.py` | sources → records → CSV (batch + streaming, single- or multi-source) |

The Transformation phase provides the `Transform` ABC, the concrete ops
(`Filter`, `MapColumn`, `Rename`, `Select`, `Aggregate`), and both the batch
composition helper `apply_transforms` and the lazy streaming helper
`stream_transforms` (plus `compose`/`Composed`). The Output phase provides
`write_csv` (batch) and `iter_write_csv` (streaming), and the `Pipeline` class
ties the phases together with `run`/`stream` (ingest + transform) and
`to_csv`/`stream_to_csv` (output). `Pipeline` accepts a single `str` path or a
`Sequence[str]` of paths for multi-source concatenation.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the phase model and conventions,
[MODULES.md](MODULES.md) for a module catalog, and [API.md](API.md) for the
public interface reference.

## Layout
