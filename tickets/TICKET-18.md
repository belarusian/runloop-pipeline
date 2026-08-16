# TICKET-18: No `compose()` helper returning a `Composed` Transform

## Title
Cycle 4 requires a `compose()` helper that folds a sequence of transforms into
a single `Composed` Transform. Neither `compose` nor a `Composed` class exists
in `pipeline/transform.py`.

## Evidence
- `grep -n "compose\|Composed" pipeline/transform.py` → no matches.
- The only composition primitive is `apply_transforms`
  (`pipeline/transform.py:261`), which is a free function that takes a list of
  transforms and a batch; it does not return a `Transform` object, so the
  result cannot itself be composed further or passed where a single
  `Transform` is expected.
- `pipeline/transform.py:34` — `Transform` is an ABC with `apply_one`/`apply`;
  there is no concrete `Composed` subclass that wraps a sequence.

## Impact
- There is no way to build a reusable, nestable transform pipeline object.
  Callers must keep passing around `list[Transform]` and re-invoke
  `apply_transforms`/`stream_transforms` each time, and a composed pipeline
  cannot be treated as a first-class `Transform` (e.g. composed with another
  op, or checked for `streamable`).
- A `Composed` object would also be the natural place to pre-compute whether
  the whole pipeline is streamable (all members streamable) for the streaming
  path (TICKET-16/17).

## Suggestion
- Add `class Composed(Transform)` wrapping a `tuple[Transform, ...]`:
  - `apply_one` chains the members' `apply_one`, dropping on the first `None`.
  - `apply` chains the members' `apply`.
  - `streamable` is `True` only if every member is streamable.
- Add `compose(*transforms: Transform) -> Composed` (or
  `compose(transforms: list[Transform]) -> Composed`) that returns a
  `Composed`.
- Export both from `pipeline/__init__.py` (TICKET-19) and document in
  `docs/API.md`.

---
_GitHub issue: https://github.com/belarusian/runloop-pipeline/issues/21
