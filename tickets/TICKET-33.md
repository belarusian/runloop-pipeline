# TICKET-33: No tests for encoding round-trips through `to_csv`/`stream_to_csv`

## Title
The output stage accepts an `encoding` argument and the ingest stage accepts an
`encoding` argument, but there is **no test** that a non-default encoding
round-trips through `Pipeline.to_csv` / `Pipeline.stream_to_csv` (or
`write_csv` / `iter_write_csv`). Non-ASCII content, BOM handling, and
`UnicodeEncodeError` surfacing as `OutputError` are all unverified at the
pipeline level.

## Evidence
- `pipeline/output.py:66` `write_csv` and `pipeline/output.py:109`
  `iter_write_csv` both take `encoding: str = "utf-8"` and open the file with
  that encoding; both catch `UnicodeEncodeError` and re-raise as
  `OutputError`.
- `pipeline/pipeline.py:209` `to_csv` and `pipeline/pipeline.py:243`
  `stream_to_csv` expose `encoding` and forward it to the writers.
- `pipeline/ingest.py` `read_csv`/`iter_rows` take `encoding: str =
  "utf-8-sig"` (BOM-stripping).
- Test coverage: `grep -rn "encoding" tests/` shows `encoding` appears only in
  monkeypatched spy signatures (`test_pipeline.py:153`,
  `test_pipeline_multi.py:82`, `test_pipeline_validate.py:110`) and in
  `test_pipeline.py:283` which only asserts the constructor *stores* the
  encoding string. There is **no** test that:
  - writes non-ASCII values (e.g. `café`, `naïve`) and reads them back equal,
  - writes with `encoding="latin-1"` and reads the bytes back as latin-1,
  - confirms a BOM-prefixed source is read and re-written without a BOM, or
  - confirms a value that cannot be encoded in the chosen encoding raises
    `OutputError` (not a bare `UnicodeEncodeError`).
- Manual check confirms the behavior works (non-ASCII round-trips, BOM is
  stripped on read and not re-emitted on write, `latin-1` bytes are correct),
  but none of this is pinned by a test.

## Impact
- A regression in encoding handling (e.g. a default changing from `utf-8` to
  `utf-8-sig`, or a BOM being re-emitted on write) would not be caught by the
  suite, and could silently corrupt non-ASCII data or produce files that
  downstream consumers mis-parse.
- The `OutputError` failure contract for `UnicodeEncodeError` (documented in
  the `write_csv`/`iter_write_csv` docstrings) is unverified; a bare
  `UnicodeEncodeError` leaking out would break the "never a bare Exception"
  contract.

## Suggestion
- Add tests (e.g. a new `tests/test_output_encoding.py` or extend
  `tests/test_output.py` and `tests/test_pipeline_multi.py`):
  - Non-ASCII round-trip: write records containing `café`/`naïve` via
    `write_csv` and via `Pipeline.to_csv`, read back with `read_csv`, assert
    equality.
  - BOM: a BOM-prefixed source read via `Pipeline` is re-written **without** a
    BOM (assert the output bytes do not start with `\xef\xbb\xbf`).
  - Non-UTF-8: `to_csv(path, encoding="latin-1")` produces latin-1 bytes;
    reading them back with `encoding="latin-1"` yields the original values.
  - `UnicodeEncodeError` → `OutputError`: writing a value that cannot be
    encoded in the chosen encoding (e.g. a non-latin-1 char with
    `encoding="latin-1"`) raises `OutputError` for both `write_csv` and
    `iter_write_csv` (the streaming case on first pull).
- Update `docs/API.md` to note the default encodings (ingest `utf-8-sig`,
  output `utf-8`) and the BOM-stripping behavior.

---
_GitHub issue: TBD
