# runloop-pipeline — documentation

A small, typed data pipeline for ingesting and transforming CSV datasets.
Records are plain `dict[str, int | float | str]` values; the pipeline is
organized as a sequence of **phases** that each consume and produce records.

## Phases

| Phase | Status | Module | In → Out |
|-------|--------|--------|----------|
| Ingestion | implemented | `pipeline/ingest.py` | CSV file → `Schema` + `list[record]` (or a lazy iterator) |
| Transformation | **Cycle 3 target — not yet implemented** | `pipeline/transform.py` (TBD) | `list[record]` → `list[record]` |
| Streaming | planned (Cycle 4) | TBD | compose per-record transforms over `iter_rows` |

See [ARCHITECTURE.md](ARCHITECTURE.md) for the phase model and conventions,
[MODULES.md](MODULES.md) for a module catalog, and [API.md](API.md) for the
public interface reference.

## Layout
