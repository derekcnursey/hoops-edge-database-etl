"""Tests for S3IO using moto mock S3."""

from __future__ import annotations

import gzip
import io
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws
import boto3

from cbbd_etl.s3_io import S3IO, make_part_key, new_run_id


@pytest.fixture()
def s3io():
    """Create S3IO instance backed by moto mock S3."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="hoops-edge")
        io_instance = S3IO("hoops-edge", "us-east-1")
        yield io_instance


class TestPutAndGetJsonGz:
    def test_put_and_get_json_gz(self, s3io: S3IO):
        """Write gzipped JSON, read it back, verify content."""
        records = [{"id": 1, "name": "Duke"}, {"id": 2, "name": "UNC"}]
        key = "raw/teams/season=2024/part-abc.json.gz"
        s3io.put_json_gz(key, records)

        raw_bytes = s3io.get_object_bytes(key)
        decompressed = gzip.decompress(raw_bytes)
        lines = decompressed.decode("utf-8").strip().split("\n")
        parsed = [json.loads(line) for line in lines]
        assert len(parsed) == 2
        assert parsed[0]["id"] == 1
        assert parsed[1]["name"] == "UNC"


class TestPutAndReadParquet:
    def test_put_and_read_parquet(self, s3io: S3IO):
        """Write a PyArrow table as Parquet, read back, verify content."""
        table = pa.table({
            "gameId": pa.array([100, 200], type=pa.int64()),
            "season": pa.array([2024, 2024], type=pa.int32()),
            "team": pa.array(["Duke", "UNC"], type=pa.string()),
        })
        key = "bronze/games/season=2024/part-abc.parquet"
        s3io.put_parquet(key, table)

        data = s3io.get_object_bytes(key)
        result = pq.read_table(io.BytesIO(data))
        assert result.num_rows == 2
        assert result.column("gameId").to_pylist() == [100, 200]
        assert result.column("team").to_pylist() == ["Duke", "UNC"]


class TestListKeys:
    def test_list_keys(self, s3io: S3IO):
        """Put multiple objects, verify list_keys returns all keys under prefix."""
        s3io.put_json_gz("raw/games/season=2024/part-001.json.gz", [{"id": 1}])
        s3io.put_json_gz("raw/games/season=2024/part-002.json.gz", [{"id": 2}])
        s3io.put_json_gz("raw/teams/season=2024/part-001.json.gz", [{"id": 3}])

        keys = s3io.list_keys("raw/games/season=2024/")
        assert len(keys) == 2
        assert "raw/games/season=2024/part-001.json.gz" in keys
        assert "raw/games/season=2024/part-002.json.gz" in keys

    def test_list_keys_empty_prefix(self, s3io: S3IO):
        """Verify empty list for nonexistent prefix."""
        keys = s3io.list_keys("nonexistent/prefix/")
        assert keys == []


class TestExists:
    def test_exists_true_and_false(self, s3io: S3IO):
        """Put an object, check exists() returns True; check nonexistent returns False."""
        key = "raw/test/file.json.gz"
        s3io.put_json_gz(key, [{"test": True}])
        assert s3io.exists(key) is True
        assert s3io.exists("raw/test/nonexistent.json.gz") is False


class TestDeleteKeys:
    def test_delete_keys(self, s3io: S3IO):
        """Put objects, delete them, verify they're gone."""
        keys = [
            "raw/games/part-001.json.gz",
            "raw/games/part-002.json.gz",
        ]
        for key in keys:
            s3io.put_json_gz(key, [{"id": 1}])

        # Verify they exist
        listed = s3io.list_keys("raw/games/")
        assert len(listed) == 2

        # Delete them
        s3io.delete_keys(keys)

        # Verify they're gone
        listed_after = s3io.list_keys("raw/games/")
        assert len(listed_after) == 0


class TestMakePartKey:
    def test_make_part_key(self):
        """Verify make_part_key produces correct path."""
        result = make_part_key("silver", "fct_games", "season=2024", "part-abc.parquet")
        assert result == "silver/fct_games/season=2024/part-abc.parquet"

    def test_make_part_key_strips_slashes(self):
        """Verify make_part_key strips extra slashes."""
        result = make_part_key("silver/", "/fct_games/", "/season=2024/", "/part-abc.parquet")
        assert result == "silver/fct_games/season=2024/part-abc.parquet"


class TestPutDeadletter:
    def test_put_deadletter(self, s3io: S3IO):
        """Write deadletter JSON, verify it's retrievable."""
        payload = {"error": "timeout", "endpoint": "games", "params": {"season": 2024}}
        key = "deadletter/games/2024-01-01_abc.json"
        s3io.put_deadletter(key, payload)

        data = s3io.get_object_bytes(key)
        recovered = json.loads(data.decode("utf-8"))
        assert recovered["error"] == "timeout"
        assert recovered["endpoint"] == "games"


class TestEnsurePrefixes:
    def test_ensure_prefixes(self, s3io: S3IO):
        """Verify .keep files are created."""
        prefixes = ["raw/", "bronze/", "silver/"]
        s3io.ensure_prefixes(prefixes)

        for prefix in prefixes:
            keep_key = prefix.rstrip("/") + "/.keep"
            assert s3io.exists(keep_key) is True


class TestNewRunId:
    def test_new_run_id_is_hex(self):
        """Verify new_run_id returns a hex UUID."""
        run_id = new_run_id()
        assert len(run_id) == 32
        int(run_id, 16)  # Should not raise
