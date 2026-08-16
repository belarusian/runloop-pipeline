# TICKET-07: Decode errors escape as raw UnicodeDecodeError, not IngestError

## Title
`read_csv` only catches `OSError` and `csv.Error`; a file whose bytes are not
valid for the target encoding raises an unhandled `UnicodeDecodeError`.

## Evidence
- `pipeline/ingest.py:36-38` — the only handlers are
  `except OSError as exc:` (line 36) and `except csv.Error as exc:` (line 38).
- `UnicodeDecodeError` is a subclass of `ValueError`, not of `OSError` or
  `csv.Error`, so it is not caught.
- The decode happens lazily inside `csv.reader` iteration at
  `pipeline/ingest.py:35` (`rows = list(csv.reader(handle))`), which is inside
  the `try` block — but no handler matches the exception type.
- `pipeline/errors.py` defines `IngestError` for "a file cannot be read or a
  CSV file is malformed", which is exactly the case a decode failure is.

## Impact
- The documented contract — "File I/O problems and malformed CSV raise
  `IngestError`" (`pipeline/ingest.py` module docstring) — is violated for
  undecodable input. Callers catching `IngestError` (or the base
  `PipelineError`) will not intercept a `UnicodeDecodeError`, so a single bad
  file crashes the pipeline with an unexpected exception type.

## Suggestion
- Add `except UnicodeDecodeError as exc:` (or broaden to `except ValueError`)
  to the `try` block at `pipeline/ingest.py:34-38` and re-raise as
  `IngestError(f"cannot decode CSV file {path!r}: {exc}") from exc`.
- Add a test: write bytes that are invalid UTF-8, assert `IngestError` is
  raised.
