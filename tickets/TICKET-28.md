# TICKET-28: No `ValidationIssue` model or rule factories (require_column/type_is/in_range/one_of)

## Title
There is no `ValidationIssue` value type to represent a single validation
failure, and no rule factories (`require_column`, `type_is`, `in_range`,
`one_of`) to express the four column-level rule kinds (presence, type, range,
membership).

## Evidence
- `grep -rn "ValidationIssue\|require_column\|type_is\|in_range\|one_of"
  pipeline/ tests/ docs/` returns no matches.
- The validation stage (TICKET-26) needs a concrete return type for
  `validate(records) -> list[ValidationIssue]` and
  `iter_validate(source) -> Iterator[ValidationIssue]`; no such type exists.
- The four rule kinds map cleanly onto existing primitives but have no
  dedicated factories:
  - *presence* — a record must contain a key; the closest existing check is
    the `if name not in record` guard in `transform.py` (`MapColumn.apply_one`
    line 110, `Select.apply_one` line 163), which *raises* rather than
    *reports*.
  - *type* — `schema.py` already classifies/coerces types
    (`Column.type`, `coerce_value` line 196), but that is inference-time, not
    a per-record rule.
  - *range* / *membership* — no existing helper compares a value against a
    `[lo, hi]` bound or an allowed set.
- Records are `dict[str, int | float | str]` (see `pipeline/pipeline.py:146`),
  so a `ValidationIssue` must carry enough context to be actionable: at least
  the record index (or a stable row id), the column name, the rule kind, the
  offending value, and a human-readable message.

## Impact
- Without a `ValidationIssue` type, `validate`/`iter_validate` (TICKET-26)
  have nothing to return; callers cannot aggregate, filter, or render issues
  (e.g. "row 42, column `age`, out of range 0..120").
- Without rule factories, every caller hand-rolls the same four checks
  inconsistently, and the rule set is not composable or inspectable.
- A `ValidationIssue` that omits the record index is useless for a batch
  report (which row failed?) and one that omits the offending value is useless
  for debugging.

## Suggestion
- Add a frozen dataclass `ValidationIssue` (in `pipeline/validate.py`) with at
  least: `index: int` (0-based record position), `column: str`, `rule: str`
  (one of `"require_column" | "type_is" | "in_range" | "one_of"`),
  `value: int | float | str | None` (the offending value; `None` when the
  column is absent), and `message: str`.
- Add four rule factories, each returning a rule object (or a
  `Callable[[record], list[ValidationIssue]]`) that the `Validator` applies per
  record:
  - `require_column(name)` — issue when `name not in record`.
  - `type_is(name, expected_type)` — issue when `record[name]` is not an
    instance of `expected_type` (guard `bool` vs `int` as `transform.py` does
    in `Aggregate._aggregate`, line 250).
  - `in_range(name, lo, hi)` — issue when `record[name]` is not `lo <= v <= hi`
    (numeric columns only).
  - `one_of(name, allowed)` — issue when `record[name]` is not in `allowed`.
- Export `ValidationIssue` and the four factories from `pipeline/__init__.py`.
- A rule that cannot be evaluated (e.g. `in_range` on a non-numeric value)
  should raise `ValidationError` (TICKET-27) at rule-construction or
  application time, not return a silent pass.

---
_GitHub issue: TBD_
