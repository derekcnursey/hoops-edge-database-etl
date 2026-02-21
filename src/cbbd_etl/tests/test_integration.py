"""Integration tests exercising multiple components together."""

from __future__ import annotations

import gzip
import io
import json

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from cbbd_etl.checkpoint import CheckpointStore
from cbbd_etl.config import Config
from cbbd_etl.glue_catalog import GlueCatalog
from cbbd_etl.normalize import normalize_records
from cbbd_etl.s3_io import S3IO


class TestS3WriteReadRoundtrip:
    def test_s3_write_read_roundtrip(self, sample_game_records):
        """Use S3IO to write game records as gzipped JSON to raw,
        then as Parquet to bronze, read back and verify content matches.
        """
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="hoops-edge")
            s3 = S3IO("hoops-edge", "us-east-1")

            # Write raw gzipped JSON
            raw_key = "raw/games/season=2024/asof=2024-01-28/part-abc.json.gz"
            s3.put_json_gz(raw_key, sample_game_records)

            # Read back and verify
            raw_bytes = s3.get_object_bytes(raw_key)
            decompressed = gzip.decompress(raw_bytes)
            lines = decompressed.decode("utf-8").strip().split("\n")
            raw_records = [json.loads(line) for line in lines]
            assert len(raw_records) == len(sample_game_records)
            assert raw_records[0]["id"] == 401596281

            # Write as Parquet to bronze
            table = normalize_records("fct_games", sample_game_records)
            bronze_key = "bronze/games/season=2024/asof=2024-01-28/part-abc.parquet"
            s3.put_parquet(bronze_key, table)

            # Read back Parquet and verify
            parquet_bytes = s3.get_object_bytes(bronze_key)
            result = pq.read_table(io.BytesIO(parquet_bytes))
            assert result.num_rows == 1
            # The API uses 'id' not 'gameId' for the game identifier
            game_ids = result.column("id").to_pylist()
            assert game_ids[0] == 401596281


class TestCheckpointRoundtripWithMoto:
    def test_checkpoint_roundtrip_with_moto(self):
        """Use real CheckpointStore with moto DynamoDB
        -- put a checkpoint, get it back, verify payload.
        """
        with mock_aws():
            ddb = boto3.client("dynamodb", region_name="us-east-1")
            ddb.create_table(
                TableName="cbbd_checkpoints",
                KeySchema=[
                    {"AttributeName": "endpoint", "KeyType": "HASH"},
                    {"AttributeName": "parameter_hash", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "endpoint", "AttributeType": "S"},
                    {"AttributeName": "parameter_hash", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )

            store = CheckpointStore("us-east-1", table_name="cbbd_checkpoints")

            payload = {
                "last_ingested_date": "2024-01-28",
                "record_count": 150,
            }
            store.put("games", "season_2024", payload)

            cp = store.get("games", "season_2024")
            assert cp is not None
            assert cp.endpoint == "games"
            assert cp.parameter_hash == "season_2024"
            assert cp.payload["last_ingested_date"] == "2024-01-28"
            assert cp.payload["record_count"] == 150

            # Non-existent checkpoint returns None
            missing = store.get("games", "nonexistent")
            assert missing is None


class TestGlueTableRegistrationWithSchema:
    def test_glue_table_registration_with_schema(self, sample_game_records):
        """Use GlueCatalog with moto Glue -- create a database,
        register a table with a PyArrow schema derived from normalize_records,
        verify the Glue table has correct columns.
        """
        with mock_aws():
            catalog = GlueCatalog("us-east-1")
            catalog.ensure_database("cbbd_silver")

            # Normalize sample records to get the schema
            table = normalize_records("fct_games", sample_game_records)
            schema = table.schema

            catalog.ensure_table(
                database="cbbd_silver",
                name="fct_games",
                location="s3://hoops-edge/silver/fct_games/",
                schema=schema,
                partition_keys=["season"],
            )

            # Verify via Glue API
            glue_client = boto3.client("glue", region_name="us-east-1")
            resp = glue_client.get_table(DatabaseName="cbbd_silver", Name="fct_games")
            glue_table = resp["Table"]

            columns = glue_table["StorageDescriptor"]["Columns"]
            col_names = {c["Name"] for c in columns}

            # Key fields from fct_games should be present
            # Note: the raw API uses 'id' not 'gameId', and normalize preserves original field names
            assert "id" in col_names
            assert "season" in col_names

            # Verify partition keys
            part_keys = [p["Name"] for p in glue_table["PartitionKeys"]]
            assert "season" in part_keys


class TestNormalizeThenWriteParquet:
    def test_normalize_then_write_parquet(self, sample_game_records):
        """Normalize sample game records, write as Parquet to moto S3,
        read back and verify schema and data.
        """
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="hoops-edge")
            s3 = S3IO("hoops-edge", "us-east-1")

            # Normalize records
            table = normalize_records("fct_games", sample_game_records)
            assert table.num_rows == 1

            # Verify schema has expected types
            # Note: the raw API uses 'id' not 'gameId', normalize preserves original names
            assert table.schema.field("id").type == pa.int64()
            assert table.schema.field("season").type == pa.int32()

            # Write to S3
            key = "silver/fct_games/season=2024/asof=2024-01-28/part-abc.parquet"
            s3.put_parquet(key, table)

            # Read back
            data = s3.get_object_bytes(key)
            result = pq.read_table(io.BytesIO(data))
            assert result.num_rows == 1
            assert result.schema.field("id").type == pa.int64()
            assert result.column("homeScore").to_pylist()[0] == 95


class TestConfigToS3IO:
    def test_config_to_s3io(self, sample_config):
        """Load config, create S3IO from it, write/read data."""
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="hoops-edge")

            s3 = S3IO(sample_config.bucket, sample_config.region)
            assert s3.bucket == "hoops-edge"
            assert s3.region == "us-east-1"

            # Write and read
            key = f"{sample_config.s3_layout['raw_prefix']}/test/data.json.gz"
            s3.put_json_gz(key, [{"test": True}])

            assert s3.exists(key) is True
            keys = s3.list_keys(f"{sample_config.s3_layout['raw_prefix']}/test/")
            assert len(keys) == 1
