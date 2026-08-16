# TICKET-10: Schema lacks projection and serialization helpers (to_dict, __len__, __iter__, project)

## Title
The `Schema` dataclass exposes only `names()`, `column()`, and `types()`; it has
no `to_dict`, `__len__`, `__iter__`, or `project` for serialization or
projection.

## Evidence
- `pipeline/schema.py:25-46` — `class Schema` defines exactly three methods:
  `names()` (line 30), `column()` (line 34), and `types()` (line 45).
- `grep -n "to_dict\|__len__\|__iter__\|project" pipeline/schema.py` returns no
  matches.
- `Schema` is a frozen `@dataclass` holding `columns: tuple[Column, ...]`
  (`pipeline/schema.py:24-26`), but there is no way to:
  - serialize it to a plain dict (e.g. for JSON/config output),
  - get its column count (`len(schema)`),
  - iterate its columns (`for col in schema`), or
  - project it down to a subset of columns (e.g. `schema.project(["id","name"])`).

## Impact
- Callers must hand-roll `{c.name: c.type for c in schema.columns}` to
  serialize, and `len(schema.columns)` to count, which is error-prone and
  inconsistent with the existing `types()` helper.
- There is no first-class way to derive a narrower schema for a subset of
  columns, so projection logic is duplicated at every call site.

## Suggestion
- Add to `Schema` (`pipeline/schema.py`):
  - `def __len__(self) -> int: return len(self.columns)`
  - `def __iter__(self) -> Iterator[Column]: return iter(self.columns)`
  - `def to_dict(self) -> dict[str, type]: return self.types()` (or a richer
    `{"columns": [{"name":..., "type":...}, ...]}` shape — pick one and
    document it)
  - `def project(self, names: Sequence[str]) -> Schema` returning a new
    `Schema` containing only the named columns, raising `SchemaError` for any
    name not present.
- Add tests for each helper, including `project` with a missing column.
