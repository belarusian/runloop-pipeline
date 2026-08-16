# TICKET-31: Multi-source `to_csv`/`stream_to_csv` silently drops columns from later sources

## Title
When a multi-source `Pipeline` is written with `to_csv`/`stream_to_csv` and no
explicit `schema`, the output column set is taken from the **first** source's
schema only. Any column that appears only in a later source is silently
dropped from the output file — its values are never written. This is a data-loss
hazard, not a documented "first source wins" contract.

## Evidence
- `pipeline/pipeline.py:209` `to_csv` and `pipeline/pipeline.py:243`
  `stream_to_csv` both fall back to `self.schema()` when `schema is None`.
- `pipeline/pipeline.py:192` `schema()` reads **only** `self._sources[0]` and
  returns that source's schema.
- `pipeline/output.py:35` `_column_order` and `pipeline/output.py:109`
  `iter_write_csv` then use that schema's `names()` as the header, and
  `pipeline/output.py:54` `_render` renders a missing column as `""`.
- Reproduction (two sources with different column sets):
