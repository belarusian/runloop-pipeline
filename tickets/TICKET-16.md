# TICKET-16: No `streamable` class attribute — batch-only `Aggregate` is not detectable or rejectable from a streaming path

## Title
Cycle 4 needs a `streamable` class attribute on `Transform` so that batch-only
ops (currently `Aggregate`) can be detected and rejected from the streaming
path. No such attribute exists on `Transform` or any concrete op.

## Evidence
- `pipeline/transform.py:34` — `class Transform(ABC):` defines only
  `apply_one` (line 44) and `apply` (line 51). There is no `streamable`
  (or equivalent) class attribute.
- `pipeline/transform.py:167` — `class Aggregate(Transform):` is batch-only;
  its `apply_one` (line 191) raises `TransformError` unconditionally. The
  only signal that it is non-streamable is that `apply_one` raises — there is
  no declarative flag a streaming path can check *before* iterating.
- `grep -n "streamable" pipeline/transform.py` → no matches.
- The per-record ops (`Filter` line 66, `MapColumn` line 90, `Rename` line
  119, `Select` line 145) are all streamable, but nothing marks them as such.

## Impact
- A streaming path (TICKET-17) cannot distinguish streamable ops from
  batch-only ops by inspection. The only way to discover that `Aggregate` is
  non-streamable is to call `apply_one` and catch the `TransformError` —
  which is a runtime failure mid-stream, not a clean rejection at
  composition time.
- A caller building a streaming pipeline that includes `Aggregate` would
  either crash partway through the stream or silently produce wrong results,
  with no upfront guard.

## Suggestion
- Add a class attribute `streamable: bool = True` to `Transform`
  (`pipeline/transform.py:34`).
- Set `streamable = False` on `Aggregate` (`pipeline/transform.py:167`).
- Keep `apply_one` raising on `Aggregate` as a second line of defense, but
  make the streaming path check `transform.streamable` up front and raise
  `TransformError` naming the offending op.
- Document the attribute in `docs/API.md` and `docs/ARCHITECTURE.md`.

---
_GitHub issue: https://github.com/belarusian/runloop-pipeline/issues/19
