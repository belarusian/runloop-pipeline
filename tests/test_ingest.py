"""Tests for the pipeline CSV reader."""

import pytest

from pipeline.errors import IngestError, SchemaError
from pipeline.ingest import iter_rows, read_csv


def test_read_csv_coerces_records(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,name,score\n1,alice,9.5\n2,bob,8.0\n")

    schema, records = read_csv(csv_file)

    assert schema.names() == ["id", "name", "score"]
    assert schema.types() == {"id": int, "name": str, "score": float}
    assert records == [
        {"id": 1, "name": "alice", "score": 9.5},
        {"id": 2, "name": "bob", "score": 8.0},
    ]
    assert isinstance(records[0]["id"], int)
    assert isinstance(records[0]["score"], float)


def test_read_csv_accepts_path_object(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("a\n1\n")
    schema, records = read_csv(csv_file)
    assert records == [{"a": 1}]


def test_read_csv_missing_file_raises(tmp_path):
    with pytest.raises(IngestError):
        read_csv(tmp_path / "does_not_exist.csv")


def test_read_csv_ragged_raises(tmp_path):
    csv_file = tmp_path / "ragged.csv"
    csv_file.write_text("a,b,c\n1,2\n3,4,5\n")
    with pytest.raises(IngestError):
        read_csv(csv_file)


def test_read_csv_empty_file_raises(tmp_path):
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("")
    with pytest.raises(IngestError):
        read_csv(csv_file)


def test_read_csv_strips_utf8_bom(tmp_path):
    csv_file = tmp_path / "bom.csv"
    # Leading UTF-8 BOM (EF BB BF) followed by a normal header.
    csv_file.write_bytes(b"\xef\xbb\xbfid,name\n1,alice\n")

    schema, records = read_csv(csv_file)

    assert schema.names() == ["id", "name"]
    assert not any(name.startswith("\ufeff") for name in schema.names())
    assert records == [{"id": 1, "name": "alice"}]


def test_read_csv_invalid_utf8_raises_ingest_error(tmp_path):
    csv_file = tmp_path / "bad.csv"
    # 0xff is not valid UTF-8.
    csv_file.write_bytes(b"a\n\xff\xfe\n")

    with pytest.raises(IngestError):
        read_csv(csv_file)


def test_iter_rows_yields_lazily_and_matches_read_csv(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,name,score\n1,alice,9.5\n2,bob,8.0\n3,carol,7.25\n")

    gen = iter_rows(csv_file)

    # The first record is available before the file is fully consumed.
    first = next(gen)
    assert first == {"id": 1, "name": "alice", "score": 9.5}
    assert isinstance(first["id"], int)
    assert isinstance(first["score"], float)

    # Drain the rest and compare against the materialized read_csv result.
    rest = list(gen)
    streamed = [first] + rest

    _, expected = read_csv(csv_file)
    assert streamed == expected


def test_iter_rows_missing_file_raises(tmp_path):
    with pytest.raises(IngestError):
        # Consuming the generator triggers the open() and raises IngestError.
        next(iter_rows(tmp_path / "does_not_exist.csv"))


def test_iter_rows_ragged_raises(tmp_path):
    csv_file = tmp_path / "ragged.csv"
    csv_file.write_text("a,b,c\n1,2\n3,4,5\n")
    with pytest.raises(IngestError):
        list(iter_rows(csv_file))


def test_iter_rows_invalid_utf8_raises(tmp_path):
    csv_file = tmp_path / "bad.csv"
    csv_file.write_bytes(b"a\n\xff\xfe\n")
    with pytest.raises(IngestError):
        list(iter_rows(csv_file))


def test_read_csv_sample_size_smaller_than_rows_still_coerces_all(tmp_path):
    # 5 data rows; sample_size=2 bounds inference to the first 2 rows.
    # The first 2 rows are numeric, later rows are strings, so the bounded
    # sample infers `value` as int. Coercion must still apply to every row,
    # which only succeeds if the column is int — proving all rows were coerced.
    csv_file = tmp_path / "mixed.csv"
    csv_file.write_text("value\n1\n2\n3\n4\n5\n")

    schema, records = read_csv(csv_file, sample_size=2)

    assert schema.types() == {"value": int}
    assert records == [{"value": v} for v in (1, 2, 3, 4, 5)]
    assert all(isinstance(r["value"], int) for r in records)


def test_read_csv_sample_size_threads_into_inference(tmp_path):
    # First 2 rows numeric, later rows strings. sample_size=2 infers `value`
    # as int from the bounded sample, so coercing the later string rows raises
    # SchemaError -- proving the bound was threaded into inference (a full scan
    # would infer str and coerce cleanly).
    csv_file = tmp_path / "mixed2.csv"
    csv_file.write_text("value\n1\n2\nx\ny\n")

    with pytest.raises(SchemaError):
        read_csv(csv_file, sample_size=2)

    # Full scan infers str, so every row coerces without error.
    full_schema, full_records = read_csv(csv_file, sample_size=None)
    assert full_schema.types() == {"value": str}
    assert full_records == [{"value": v} for v in ("1", "2", "x", "y")]
