# TICKET-05: No public API exports and no documentation

## Title
`pipeline/__init__.py` exports nothing beyond `__version__`, and there is no README or docs/ describing the package.

## Evidence
- `pipeline/__init__.py` (lines 1-3) exposes only `__version__`; no `__all__`, no re-exports of reader/Schema/errors.
- No `README.md` at repo root (`cat README.md` returns nothing).
- No `docs/` directory exists.
- `pyproject.toml` `description` promises "ingesting and transforming CSV datasets" but no module fulfills it.

## Impact
Even once TICKET-01/02/03 land, consumers have no documented entry points and no way to discover the public API. Newcomers landing at the repo have no onboarding docs.

## Suggestion
- Add `__all__` and re-export the public API (reader, `Schema`, `PipelineError` + subtypes) from `pipeline/__init__.py`.
- Add `README.md` (quickstart: read a CSV, get a typed result) and `docs/MODULES.md` cataloging `ingest`, `schema`, `errors` and their relationships.
