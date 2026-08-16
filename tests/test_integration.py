"""End-to-end integration tests for the pipeline package (Cycle 8).

These tests exercise the full documented flow — ingest -> transform ->
output (and validation) — over real, temporary multi-file CSVs, so that
cross-phase interactions are pinned, not just each phase in isolation.

Every output is read back with :func:`read_csv` (or a raw ``csv.reader``) so
the round-trip is self-checking.

Scenarios:
1. Full happy path: two sources, a ``Filter`` + ``MapColumn`` + ``Rename``
   chain, ``to_csv``, read back, assert exact records and column order.
2. Streaming parity: ``stream_to_csv`` produces a byte-identical file to
   ``to_csv`` over the same dataset.
3. Validation gate: ``validate`` over ``[require_column, type_is, in_range,
   one_of]`` returns the hand-computed issues; ``iter_validate`` yields the
   same issues in the same order.
4. Gate-before-write: when ``validate`` returns issues the file is NOT
   written; when it returns ``[]`` ``to_csv`` succeeds and the row count
   matches.
5. Multi-source with differing column sets: ``to_csv`` with an explicit
   schema yields a stable column order and missing columns render as empty
   strings.
6. Empty pipeline: a header-only source yields ``run() == []``, ``to_csv``
   writes a header-only file and returns 0, and ``validate`` returns ``[]``.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from pipeline.errors import IngestError, OutputError, TransformError
from pipeline.ingest import read_csv
from pipeline.pipeline import Pipeline
from pipeline.schema import Column, Schema
from pipeline.transform import Aggregate, Filter, MapColumn, Rename
from pipeline.validation import (
    ValidationIssue,
    in_range,
    one_of,
    require_column,
    type_is,
)


def _write_csv(tmp_path: Path, text: str, name: str) -> str:
    """Write *text* to ``tmp_path / name`` and return the path as a string."""
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return str(path)


def _read_raw(path: Path) -> list[list[str]]:
    """Read a CSV file back as raw string cells (no coercion)."""
    with open(path, "r", newline="", encoding="utf-8") as handle:
        return list(csv.reader(handle))


def _happy_path_pipeline(tmp_path: Path) -> Pipeline:
    """Build the two-source happy-path pipeline used by scenarios 1 and 2."""
    a = _write_csv(
        tmp_path,
        "id,name,score\n"
        "1,alice,9.5\n"
        "2,bob,8.0\n",
        "a.csv",
    )
    b = _write_csv(
        tmp_path,
        "id,name,score\n"
        "3,carol,7.25\n"
        "4,dave,6.5\n",
        "b.csv",
    )
    return Pipeline(
        [a, b],
        [
            Filter(lambda r: r["score"] >= 7.0),  # drop dave (6.5)
            MapColumn("score", lambda v: v * 2),
            Rename("name", "label"),
        ],
    )


# ---------------------------------------------------------------------------
# 1. Full happy path
# ---------------------------------------------------------------------------


def test_happy_path_two_sources_transform_to_csv_round_trip(tmp_path):
    pipeline = _happy_path_pipeline(tmp_path)
    out_path = tmp_path / "out.csv"

    # The explicit schema fixes the output column order to the transformed
    # record keys (id, label, score).
    schema = Schema(
        columns=(
            Column("id", int),
            Column("label", str),
            Column("score", float),
        )
    )
    written = pipeline.to_csv(str(out_path), schema=schema)

    assert written == 3
    # Read the output back with read_csv so the round-trip is self-checking.
    read_schema, read_back = read_csv(str(out_path))
    assert read_schema.names() == ["id", "label", "score"]
    assert read_back == [
        {"id": 1, "label": "alice", "score": 19.0},
        {"id": 2, "label": "bob", "score": 16.0},
        {"id": 3, "label": "carol", "score": 14.5},
    ]
    # Column order in the file matches the explicit schema.
    assert _read_raw(out_path)[0] == ["id", "label", "score"]


# ---------------------------------------------------------------------------
# 2. Streaming parity
# ---------------------------------------------------------------------------


def test_stream_to_csv_is_byte_identical_to_to_csv(tmp_path):
    pipeline = _happy_path_pipeline(tmp_path)
    schema = Schema(
        columns=(
            Column("id", int),
            Column("label", str),
            Column("score", float),
        )
    )
    batch_path = tmp_path / "batch.csv"
    stream_path = tmp_path / "stream.csv"

    batch_count = pipeline.to_csv(str(batch_path), schema=schema)
    stream_count = pipeline.stream_to_csv(str(stream_path), schema=schema)

    assert batch_count == stream_count == 3
    assert batch_path.read_bytes() == stream_path.read_bytes()


# ---------------------------------------------------------------------------
# 3. Validation gate (batch + streaming parity)
# ---------------------------------------------------------------------------


def test_validate_matches_hand_computed_issues_and_iter_validate_agrees(tmp_path):
    a = _write_csv(
        tmp_path,
        "id,score,tag\n"
        "1,10,a\n"
        "2,150,b\n",
        "a.csv",
    )
    # Source B has no "tag" column, so its records lack it.
    b = _write_csv(tmp_path, "id,score\n3,5\n", "b.csv")

    pipeline = Pipeline([a, b])
    rules = [
        require_column("tag"),
        type_is("id", int),
        in_range("score", 0, 100),
        one_of("tag", ["a", "b"]),
    ]

    issues = pipeline.validate(rules)

    # Hand-computed, record-major order:
    #   row 0 {id:1, score:10, tag:a}  -> all pass
    #   row 1 {id:2, score:150, tag:b} -> in_range fails (150 > 100)
    #   row 2 {id:3, score:5}          -> require_column fails (no tag)
    assert issues == [
        ValidationIssue(
            row=1,
            column="score",
            message="column 'score' has value 150 outside range [0, 100]",
        ),
        ValidationIssue(row=2, column="tag", message="missing required column 'tag'"),
    ]

    # iter_validate yields the same issues in the same order.
    streamed = list(pipeline.iter_validate(rules))
    assert streamed == issues
    assert [i.row for i in streamed] == [1, 2]
    assert [i.column for i in streamed] == ["score", "tag"]


# ---------------------------------------------------------------------------
# 4. Gate-before-write workflow
# ---------------------------------------------------------------------------


def test_gate_before_write_writes_only_when_validation_is_clean(tmp_path):
    # A source whose data passes the rules.
    clean = _write_csv(tmp_path, "id,score\n1,10\n2,20\n", "clean.csv")
    # A source whose data fails the rules (score 150 is out of range).
    dirty = _write_csv(tmp_path, "id,score\n1,10\n2,150\n", "dirty.csv")
    rules = [require_column("id"), in_range("score", 0, 100)]

    # Clean pipeline: validate returns [] -> to_csv writes and returns count.
    clean_path = tmp_path / "clean_out.csv"
    clean_pipeline = Pipeline(clean)
    clean_issues = clean_pipeline.validate(rules)
    assert clean_issues == []
    clean_written = clean_pipeline.to_csv(str(clean_path))
    assert clean_written == 2
    assert clean_path.exists()

    # Dirty pipeline: validate returns issues -> do NOT write.
    dirty_path = tmp_path / "dirty_out.csv"
    dirty_pipeline = Pipeline(dirty)
    dirty_issues = dirty_pipeline.validate(rules)
    assert dirty_issues != []
    if not dirty_issues:
        dirty_pipeline.to_csv(str(dirty_path))
    assert not dirty_path.exists()


# ---------------------------------------------------------------------------
# 5. Multi-source with differing column sets (explicit schema)
# ---------------------------------------------------------------------------


def test_multi_source_differing_columns_explicit_schema_stable_order(tmp_path):
    a = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n", "a.csv")
    b = _write_csv(tmp_path, "id,score\n3,9.5\n4,8.0\n", "b.csv")

    pipeline = Pipeline([a, b])
    schema = Schema(
        columns=(
            Column("id", int),
            Column("name", str),
            Column("score", float),
        )
    )
    out_path = tmp_path / "out.csv"

    written = pipeline.to_csv(str(out_path), schema=schema)
    assert written == 4

    # Stable column order; columns absent from a record render as "".
    rows = _read_raw(out_path)
    assert rows == [
        ["id", "name", "score"],
        ["1", "alice", ""],
        ["2", "bob", ""],
        ["3", "", "9.5"],
        ["4", "", "8.0"],
    ]


# ---------------------------------------------------------------------------
# 6. Empty pipeline (header-only source)
# ---------------------------------------------------------------------------


def test_empty_pipeline_header_only_source(tmp_path):
    src = _write_csv(tmp_path, "id,name,score\n", "empty.csv")
    pipeline = Pipeline(src)

    assert pipeline.run() == []

    out_path = tmp_path / "out.csv"
    written = pipeline.to_csv(str(out_path))
    assert written == 0
    # A header-only file: the header row is present, no data rows.
    assert _read_raw(out_path) == [["id", "name", "score"]]

    # Validation over an empty result yields no issues.
    assert pipeline.validate([require_column("id"), in_range("score", 0, 100)]) == []


# ---------------------------------------------------------------------------
# 7. Encoding round-trips through Pipeline.to_csv (issue #45)
# ---------------------------------------------------------------------------


def test_pipeline_to_csv_non_ascii_round_trips(tmp_path):
    src = _write_csv(tmp_path, "id,name\n1,caf\u00e9\n2,na\u00efve\n", "src.csv")
    out_path = tmp_path / "out.csv"

    pipeline = Pipeline(src)
    written = pipeline.to_csv(str(out_path))
    assert written == 2

    _, read_back = read_csv(str(out_path))
    assert read_back == [
        {"id": 1, "name": "caf\u00e9"},
        {"id": 2, "name": "na\u00efve"},
    ]


def test_pipeline_to_csv_bom_prefixed_source_rewritten_without_bom(tmp_path):
    # A BOM-prefixed source (EF BB BF) read via Pipeline.
    src_path = tmp_path / "src.csv"
    src_path.write_bytes(b"\xef\xbb\xbf" + "id,name\n1,alice\n".encode("utf-8"))

    out_path = tmp_path / "out.csv"
    pipeline = Pipeline(str(src_path))
    written = pipeline.to_csv(str(out_path))
    assert written == 1

    # The output must NOT start with a UTF-8 BOM.
    out_bytes = out_path.read_bytes()
    assert not out_bytes.startswith(b"\xef\xbb\xbf")
    _, read_back = read_csv(str(out_path))
    assert read_back == [{"id": 1, "name": "alice"}]


def test_pipeline_to_csv_latin1_round_trips(tmp_path):
    src = _write_csv(tmp_path, "id,name\n1,caf\u00e9\n", "src.csv")
    out_path = tmp_path / "out.csv"

    pipeline = Pipeline(src)
    written = pipeline.to_csv(str(out_path), encoding="latin-1")
    assert written == 1

    # The bytes are valid latin-1 and decode back to the original value.
    raw = out_path.read_bytes()
    assert raw.decode("latin-1") == "id,name\r\n1,caf\u00e9\r\n"
    _, read_back = read_csv(str(out_path), encoding="latin-1")
    assert read_back == [{"id": 1, "name": "caf\u00e9"}]


def test_pipeline_to_csv_unencodable_value_raises_output_error(tmp_path):
    # The euro sign (U+20AC) is not representable in latin-1.
    src = _write_csv(tmp_path, "id,name\n1,1\u20ac\n", "src.csv")
    out_path = tmp_path / "out.csv"

    pipeline = Pipeline(src)
    with pytest.raises(OutputError):
        pipeline.to_csv(str(out_path), encoding="latin-1")


# ---------------------------------------------------------------------------
# 8. Gate-before-write: the documented manual recipe (issue #47)
# ---------------------------------------------------------------------------


def test_gate_before_write_documented_recipe(tmp_path):
    """Mirror the documented recipe: validate first, write only if clean.

    issues = pipeline.validate(rules)
    if not issues:
        pipeline.to_csv(path)

    A pipeline whose data passes the rules writes the file and returns the
    row count; a pipeline whose data fails the rules does NOT write the file.
    """
    rules = [require_column("id"), in_range("score", 0, 100)]

    # Passing data: the gate opens and to_csv writes the file.
    passing = _write_csv(tmp_path, "id,score\n1,10\n2,20\n3,30\n", "passing.csv")
    passing_path = tmp_path / "passing_out.csv"
    passing_pipeline = Pipeline(passing)
    issues = passing_pipeline.validate(rules)
    assert issues == []
    written = None
    if not issues:
        written = passing_pipeline.to_csv(str(passing_path))
    assert written == 3
    assert passing_path.exists()

    # Failing data: the gate stays closed and the file is never written.
    failing = _write_csv(tmp_path, "id,score\n1,10\n2,150\n", "failing.csv")
    failing_path = tmp_path / "failing_out.csv"
    failing_pipeline = Pipeline(failing)
    issues = failing_pipeline.validate(rules)
    assert issues != []
    if not issues:
        failing_pipeline.to_csv(str(failing_path))
    assert not failing_path.exists()


# ---------------------------------------------------------------------------
# 9. Error-path integration (issue #46 error-path row)
# ---------------------------------------------------------------------------


def test_decode_failure_raises_ingest_error_from_run_and_to_csv(tmp_path):
    # A source with bytes that are not valid UTF-8 fails to decode.
    src_path = tmp_path / "bad.csv"
    src_path.write_bytes(b"id,name\n1,\xff\xfe\xfd\n")

    pipeline = Pipeline(str(src_path))

    with pytest.raises(IngestError):
        pipeline.run()
    with pytest.raises(IngestError):
        pipeline.to_csv(str(tmp_path / "out.csv"))


def test_non_streamable_aggregate_works_batch_but_raises_streaming(tmp_path):
    src = _write_csv(
        tmp_path,
        "id,grp,val\n"
        "1,a,10\n"
        "2,a,20\n"
        "3,b,5\n",
        "agg.csv",
    )
    pipeline = Pipeline(src, [Aggregate(["grp"], {"val": "sum"})])

    # Batch path works: run() and to_csv() succeed.
    assert pipeline.run() == [{"grp": "a", "val": 30}, {"grp": "b", "val": 5}]
    batch_out = tmp_path / "batch.csv"
    assert pipeline.to_csv(str(batch_out)) == 2

    # Streaming paths reject the batch-only op with TransformError.
    with pytest.raises(TransformError):
        pipeline.stream_to_csv(str(tmp_path / "stream.csv"))
    with pytest.raises(TransformError):
        list(pipeline.iter_validate([require_column("grp")]))


def test_unwritable_path_raises_output_error_from_to_csv_and_stream_to_csv(tmp_path):
    src = _write_csv(tmp_path, "id,name\n1,alice\n", "data.csv")
    # A directory cannot be opened for writing.
    directory = tmp_path / "subdir"
    directory.mkdir()

    pipeline = Pipeline(src)

    with pytest.raises(OutputError):
        pipeline.to_csv(str(directory))
    with pytest.raises(OutputError):
        pipeline.stream_to_csv(str(directory))
