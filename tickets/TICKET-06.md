# TICKET-06: CSV reader does not handle encoding or a UTF-8 BOM

## Title
`read_csv` opens the file with a hard-coded text mode and no `encoding`, so a
UTF-8 BOM corrupts the first header name and non-UTF-8 files cannot be read.

## Evidence
- `pipeline/ingest.py:34` — `with open(path, "r", newline="") as handle:`. There is
  no `encoding=` argument, so Python falls back to the platform default
  (`locale.getpreferredencoding()`), which is not guaranteed to be UTF-8.
- There is no `utf-8-sig` handling. A file beginning with the BOM bytes
  `EF BB BF` decodes the first header cell as `"\ufeffid"` instead of `"id"`,
  so `schema.names()[0]` and every record key for that column are silently
  wrong.
- `grep -n "encoding\|utf-8-sig\|BOM" pipeline/ingest.py` returns no matches.

## Impact
- Files exported by Excel/Windows tools (commonly UTF-8 with BOM) produce a
  schema whose first column name is wrong, breaking downstream lookups such as
  `schema.column("id")` (raises `SchemaError`) and record-key access.
- Non-UTF-8 files (e.g. Latin-1) either raise an unhandled
  `UnicodeDecodeError` (see TICKET-07) or, if the platform default happens to
  match, read silently with no caller control.

## Suggestion
- Add an `encoding: str = "utf-8-sig"` parameter to `read_csv` and pass it to
  `open(...)`. `utf-8-sig` transparently strips a leading BOM and is a no-op
  for plain UTF-8, so it is a safe default.
- Document the parameter in the `read_csv` docstring (Args section).
- Add a test: write a CSV with a leading BOM, assert the first header name is
  clean (no `\ufeff`).

---
_GitHub issue: https://github.com/belarusian/runloop-pipeline/issues/7_
