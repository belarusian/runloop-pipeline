# runloop-pipeline — documentation

A small, typed data pipeline for ingesting and transforming CSV datasets.
Records are plain `dict[str, int | float | str]` values; the pipeline is
organized as a sequence of **phases** that each consume and produce records.

## Phases

| Phase | Status | Module | In → Out |
|-------|--------|--------|----------|
| Ingestion | implemented | `pipeline/ingest.py` | CSV file → `Schema` + `list[record]` (or a lazy iterator) |
| Transformation | implemented | `pipeline/transform.py` | `list[record]` → `list[record]` |
| Streaming + Composition | **Cycle 4 target — not yet implemented** | `pipeline/transform.py` (TBD) | `Iterator[record]` → `Iterator[record]` |

The Transformation phase provides the `Transform` ABC, the concrete ops
(`Filter`, `MapColumn`, `Rename`, `Select`, `Aggregate`), and the batch
composition helper `apply_transforms`. The Cycle 4 target adds the streaming
and composition surface: a `streamable` class attribute, a lazy
`stream_transforms` generator over an `Iterator[dict]` source (e.g.
`iter_rows`), and a `compose()` helper returning a `Composed` Transform. See
tickets `TICKET-16` … `TICKET-20`.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the phase model and conventions,
[MODULES.md](MODULES.md) for a module catalog, and [API.md](API.md) for the
public interface reference.

## Layout
