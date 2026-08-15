# TICKET-02: Schema dataclass and type inference are missing

## Title
No `Schema` dataclass (column name + type) and no schema inference from sample rows.

## Evidence
- `pipeline/__init__.py` (lines 1-3) defines no `Schema` type.
- No module in `pipeline/` defines a `Schema` dataclass or an `infer_schema(rows)` function.
- `grep -rn "Schema" pipeline/` returns no matches (only `__init__.py` exists).

## Impact
Without a `Schema` dataclass and inference logic, the reader (TICKET-01) has no way to represent column types or to decide int/float/str coercion. Downstream transforms have no typed contract to rely on.

## Suggestion
Add a `Schema` dataclass (e.g. `pipeline/schema.py`):
- `Column(name: str, type: type)` and `Schema(columns: list[Column])`.
- `infer_schema(sample_rows) -> Schema` that inspects sample values and classifies each column as `int`, `float`, or `str` (int if all parse as int; float if all parse as float; else str).
- Raise `SchemaError` (TICKET-03) on ambiguous/empty inference.
