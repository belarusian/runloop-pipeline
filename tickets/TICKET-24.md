# TICKET-24: No schema reconciliation contract for multi-source concatenation

## Title
If `Pipeline` is extended to accept multiple sources (TICKET-23), there is no
defined contract for how differing headers and column types across sources are
reconciled into a single output schema.

## Evidence
- `pipeline/schema.py` infers a schema from a *single* sample:
  `infer_schema(sample_rows, header=None, sample_size=None)` (line 108) takes
  one `header` list and one set of `sample_rows`. There is no function that
  merges two `Schema` objects or reconciles two headers.
- `Schema` is a frozen dataclass holding `columns: tuple[Column, ...]`
  (`pipeline/schema.py:24-26`). It has `project(names)` (line 59) for
  narrowing, but no `union`/`merge`/`reconcile` for combining two schemas.
- `pipeline/ingest.py` coerces each row against the schema inferred from that
  file's own sample (`read_csv` line 33, `iter_rows` line 91). Two files with
  the same column name but different inferred types (e.g. `id` inferred as
  `int` in `a.csv` but `str` in `b.csv`) would be coerced independently, so a
  naive concatenation mixes `int` and `str` values under the same key.
- `grep -rn "union\|merge\|reconcile\|combine" pipeline/schema.py` returns no
  matches.

## Impact
- Multi-source concatenation (TICKET-23) is underspecified: the output schema
  is ambiguous when sources disagree on (a) which columns are present, (b)
  column order, or (c) a column's type.
- Without a contract, a naive `list(a_records) + list(b_records)` produces
  records whose keys and value types are inconsistent across the combined list,
  silently corrupting downstream transforms (e.g. `MapColumn` on a column that
  is `int` in one source and `str` in another).
- The streaming path cannot infer a single up-front schema across lazily-chained
  sources without a defined reconciliation rule, so `stream()` over multiple
  sources has no well-defined behavior.

## Suggestion
- Define the reconciliation contract in `docs/API.md` and implement it in
  `pipeline/schema.py`:
  - `Schema.union(other: Schema) -> Schema` — combine two schemas. Proposed
    rule (pick one and document it): columns are the union of both, in
    first-seen order (source A's order, then any columns only in source B); a
    column present in both keeps source A's type; a type conflict (same name,
    different type) raises `SchemaError` rather than silently widening.
  - Missing columns in a given source are filled with a sentinel (e.g. `None`
    or `""`) so every record in the combined stream has the same keys.
- Add `read_csv_many`/`iter_rows_many` (TICKET-23) to infer the *combined*
  schema first (union of per-source inferred schemas), then coerce every row
  against that single combined schema.
- Add tests: identical headers (no-op union), disjoint headers (union in
  order), a type conflict raising `SchemaError`, and a missing column filled
  with the documented sentinel.

---
_GitHub issue: https://github.com/belarusian/runloop-pipeline/issues/34_
