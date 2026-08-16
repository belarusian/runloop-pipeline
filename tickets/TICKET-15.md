# TICKET-15: `Aggregate` op semantics are underspecified (agg signature, types, output shape)

## Title
The required `Aggregate(group_by, agg)` op has no defined semantics for the
aggregation function signature, the type constraints on aggregated columns, or
the shape of the output row. This is a design gap that blocks implementation.

## Evidence
- The spec requires `Aggregate(group_by, agg)` "producing one row per group",
  but does not define:
  - the type of `agg` (a `dict[str, Callable]`? which callables — `sum`, `len`,
    `min`, `max`, or arbitrary callables?),
  - the signature of an aggregation callable (does it take the list of values
    for a column, or a single value?),
  - how a group's output row is keyed (the `group_by` values plus the
    aggregated columns?), and
  - what happens when an aggregation is applied to a non-numeric column (e.g.
    `sum` over `str`), given records are `dict[str, int | float | str]`
    (`pipeline/ingest.py:44`).
- `pipeline/schema.py` types columns as `int`, `float`, or `str`, so an
  aggregation like `sum` is only well-defined for `int`/`float` columns. There
  is no existing helper that distinguishes numeric from string columns for this
  purpose.
- No reference implementation or test exists to anchor the intended behavior.

## Impact
- Implementers would have to invent the `agg` contract, the output row shape,
  and the failure behavior independently, producing an op that is hard to test
  and hard to compose with the other (per-record) ops.
- An undefined failure mode (e.g. `sum` over a `str` column) risks raising a
  bare `TypeError`/`ValueError` instead of the required `TransformError`
  (TICKET-12), breaking the "never a bare `Exception`" contract.

## Suggestion
- Specify `Aggregate` precisely in `docs/API.md`:
  - `agg: dict[str, Callable[[list[int | float | str]], int | float | str]]`
    mapping a column name to a function over that column's values within a
    group (e.g. `sum`, `len`, `min`, `max`);
  - the output row is `{**group_by_values, **{col: fn(values) for col, fn in
    agg.items()}}`, one row per distinct `group_by` key (in first-seen order);
  - applying a numeric aggregation to a non-numeric column raises
    `TransformError` (chaining the original exception).
- Add tests for: a single group, multiple groups, `len` over a `str` column,
  and a numeric aggregation over a `str` column raising `TransformError`.

---
_GitHub issue: https://github.com/belarusian/runloop-pipeline/issues/17_
