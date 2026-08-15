"""Tests for the pipeline CSV reader."""

import pytest

from pipeline.errors import IngestError
from pipeline.ingest import read_csv


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
