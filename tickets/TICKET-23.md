# TICKET-23: `Pipeline` accepts only a single `str` source (no multi-source composition)

## Title
`Pipeline` is hard-wired to a single CSV source. It cannot ingest and
concatenate multiple CSV sources, so combining several files requires external
glue.

## Evidence
- `pipeline/pipeline.py:58-66` — `__init__` signature is
  `source: str` (line 60), stored as `self._source = source` (line 66).
  The `source` property (line 73) returns a single `str`.
- Every entry point reads exactly one path:
  - `run()` calls `read_csv(self._source, ...)` (line 106).
  - `stream()` calls `iter_rows(self._source, ...)` (line 126).
  - `schema()` calls `read_csv(self._source, ...)` (line 143).
- `grep -rn "concat\|sources\|Sequence\[str\]\|Iterable\[str\]\|list\[str\]"
  pipeline/pipeline.py` returns no matches — there is no collection-of-sources
  type anywhere in the class.
- `pipeline/ingest.py` exposes `read_csv`/`iter_rows` for a single `path`
  (lines 33, 91); there is no `read_csv_many` / `iter_rows_many` helper.

## Impact
- A common real-world shape — "concatenate `a.csv`, `b.csv`, `c.csv`, then
  transform" — cannot be expressed with `Pipeline` alone. The caller must
  read each file, concatenate the record lists, and re-wrap them, bypassing the
  package's schema inference and the streaming path entirely.
- The streaming path (`stream()`) cannot be used for multi-source concatenation
  at all, because `iter_rows` is single-file; the caller is forced to
  materialize every source into memory to concatenate, defeating the
  bounded-memory design of `stream()`.
- The `source` property's type (`str`) and the `Pipeline` docstring
  ("ingest a source") both encode the single-source assumption, so the public
  contract would need to change to support multiple sources.

## Suggestion
- Generalize the source to a sequence of paths while preserving backward
  compatibility:
  - Accept `source: str | Sequence[str]` in `__init__` (line 60); normalize a
    bare `str` to a one-element list internally.
  - Add a `sources` property returning the normalized `list[str]`; keep
    `source` as a convenience that raises (or returns the first) when more than
    one source is present — pick one and document it.
- Add multi-source ingestion to `pipeline/ingest.py`:
  - `read_csv_many(paths, encoding, sample_size) -> (Schema, list[record])`
  - `iter_rows_many(paths, encoding, sample_size) -> Iterator[record]`
    (lazily chains `iter_rows` over each path so the streaming path stays
    bounded-memory).
- Define the concatenation contract (see `TICKET-24`) for how differing
  headers/types across sources are reconciled.
- Update `run()`/`stream()`/`schema()` to call the `_many` variants.
- Add tests: two sources with identical headers concatenate in order; a bare
  `str` source behaves exactly as before (regression); streaming over two
  sources is lazy (pulling one record does not read the second file).

---
_GitHub issue: https://github.com/belarusian/runloop-pipeline/issues/33_
