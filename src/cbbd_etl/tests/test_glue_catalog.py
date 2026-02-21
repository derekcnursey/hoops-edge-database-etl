"""Tests for Glue catalog management using moto."""

from __future__ import annotations

import boto3
import pyarrow as pa
import pytest
from moto import mock_aws

from cbbd_etl.glue_catalog import (
    GlueCatalog,
    _pa_to_glue,
    _table_matches,
    _normalize_columns,
)


@pytest.fixture()
def glue_catalog():
    """Create a GlueCatalog instance backed by moto mock Glue."""
    with mock_aws():
        catalog = GlueCatalog("us-east-1")
        yield catalog


class TestEnsureDatabase:
    def test_ensure_database_creates(self, glue_catalog: GlueCatalog):
        """Verify new database is created."""
        glue_catalog.ensure_database("cbbd_test")
        # Verify it exists by calling get_database (should not raise)
        glue_catalog._client.get_database(Name="cbbd_test")

    def test_ensure_database_idempotent(self, glue_catalog: GlueCatalog):
        """Call twice, no error."""
        glue_catalog.ensure_database("cbbd_test")
        glue_catalog.ensure_database("cbbd_test")
        # Should still exist without error
        resp = glue_catalog._client.get_database(Name="cbbd_test")
        assert resp["Database"]["Name"] == "cbbd_test"


class TestEnsureTable:
    def test_ensure_table_creates(self, glue_catalog: GlueCatalog):
        """Create table with PyArrow schema, verify via Glue get_table."""
        glue_catalog.ensure_database("cbbd_test")

        schema = pa.schema([
            pa.field("gameId", pa.int64()),
            pa.field("season", pa.int32()),
            pa.field("team", pa.string()),
        ])
        glue_catalog.ensure_table(
            database="cbbd_test",
            name="fct_games",
            location="s3://hoops-edge/silver/fct_games/",
            schema=schema,
            partition_keys=["season"],
        )

        resp = glue_catalog._client.get_table(DatabaseName="cbbd_test", Name="fct_games")
        table = resp["Table"]
        columns = table["StorageDescriptor"]["Columns"]
        col_names = [c["Name"] for c in columns]
        assert "gameId" in col_names
        assert "season" in col_names
        assert "team" in col_names

        partitions = table["PartitionKeys"]
        assert any(p["Name"] == "season" for p in partitions)

    def test_ensure_table_updates_on_schema_change(self, glue_catalog: GlueCatalog):
        """Create table, then call again with different schema, verify update."""
        glue_catalog.ensure_database("cbbd_test")

        schema_v1 = pa.schema([
            pa.field("gameId", pa.int64()),
            pa.field("team", pa.string()),
        ])
        glue_catalog.ensure_table(
            database="cbbd_test",
            name="fct_games",
            location="s3://hoops-edge/silver/fct_games/",
            schema=schema_v1,
        )

        # Update with new column
        schema_v2 = pa.schema([
            pa.field("gameId", pa.int64()),
            pa.field("team", pa.string()),
            pa.field("season", pa.int32()),
        ])
        glue_catalog.ensure_table(
            database="cbbd_test",
            name="fct_games",
            location="s3://hoops-edge/silver/fct_games/",
            schema=schema_v2,
        )

        resp = glue_catalog._client.get_table(DatabaseName="cbbd_test", Name="fct_games")
        columns = resp["Table"]["StorageDescriptor"]["Columns"]
        col_names = [c["Name"] for c in columns]
        assert "season" in col_names
        assert len(columns) == 3

    def test_ensure_table_idempotent(self, glue_catalog: GlueCatalog):
        """Call twice with same schema, verify no error."""
        glue_catalog.ensure_database("cbbd_test")

        schema = pa.schema([
            pa.field("gameId", pa.int64()),
            pa.field("team", pa.string()),
        ])
        glue_catalog.ensure_table(
            database="cbbd_test",
            name="fct_games",
            location="s3://hoops-edge/silver/fct_games/",
            schema=schema,
        )
        # Second call should be a no-op
        glue_catalog.ensure_table(
            database="cbbd_test",
            name="fct_games",
            location="s3://hoops-edge/silver/fct_games/",
            schema=schema,
        )
        resp = glue_catalog._client.get_table(DatabaseName="cbbd_test", Name="fct_games")
        assert resp["Table"]["Name"] == "fct_games"


class TestPaToGlue:
    @pytest.mark.parametrize(
        "pa_type,expected_glue",
        [
            (pa.int64(), "bigint"),
            (pa.int32(), "int"),
            (pa.float64(), "double"),
            (pa.bool_(), "boolean"),
            (pa.timestamp("s"), "timestamp"),
            (pa.timestamp("ms"), "timestamp"),
            (pa.string(), "string"),
            (pa.utf8(), "string"),
        ],
    )
    def test_pa_to_glue_type_mapping(self, pa_type, expected_glue):
        """Test _pa_to_glue for various PyArrow types."""
        assert _pa_to_glue(pa_type) == expected_glue


class TestTableMatches:
    def test_table_matches_matching(self):
        """Test _table_matches with matching table definitions."""
        existing = {
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "gameId", "Type": "bigint"},
                    {"Name": "team", "Type": "string"},
                ],
                "Location": "s3://hoops-edge/silver/fct_games/",
            },
            "PartitionKeys": [{"Name": "season", "Type": "string"}],
        }
        desired = {
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "gameId", "Type": "bigint"},
                    {"Name": "team", "Type": "string"},
                ],
                "Location": "s3://hoops-edge/silver/fct_games/",
            },
            "PartitionKeys": [{"Name": "season", "Type": "string"}],
        }
        assert _table_matches(existing, desired) is True

    def test_table_matches_non_matching_columns(self):
        """Test _table_matches with different columns."""
        existing = {
            "StorageDescriptor": {
                "Columns": [{"Name": "gameId", "Type": "bigint"}],
                "Location": "s3://hoops-edge/silver/fct_games/",
            },
            "PartitionKeys": [],
        }
        desired = {
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "gameId", "Type": "bigint"},
                    {"Name": "team", "Type": "string"},
                ],
                "Location": "s3://hoops-edge/silver/fct_games/",
            },
            "PartitionKeys": [],
        }
        assert _table_matches(existing, desired) is False

    def test_table_matches_non_matching_location(self):
        """Test _table_matches with different location."""
        existing = {
            "StorageDescriptor": {
                "Columns": [{"Name": "gameId", "Type": "bigint"}],
                "Location": "s3://hoops-edge/silver/fct_games/",
            },
            "PartitionKeys": [],
        }
        desired = {
            "StorageDescriptor": {
                "Columns": [{"Name": "gameId", "Type": "bigint"}],
                "Location": "s3://hoops-edge/silver/fct_lines/",
            },
            "PartitionKeys": [],
        }
        assert _table_matches(existing, desired) is False

    def test_table_matches_trailing_slash_insensitive(self):
        """Trailing slash difference should still match."""
        existing = {
            "StorageDescriptor": {
                "Columns": [{"Name": "gameId", "Type": "bigint"}],
                "Location": "s3://hoops-edge/silver/fct_games",
            },
            "PartitionKeys": [],
        }
        desired = {
            "StorageDescriptor": {
                "Columns": [{"Name": "gameId", "Type": "bigint"}],
                "Location": "s3://hoops-edge/silver/fct_games/",
            },
            "PartitionKeys": [],
        }
        assert _table_matches(existing, desired) is True


class TestNormalizeColumns:
    def test_normalize_columns(self):
        """Test _normalize_columns case normalization."""
        columns = [
            {"Name": "GameId", "Type": "BIGINT"},
            {"Name": "Team", "Type": "STRING"},
        ]
        result = _normalize_columns(columns)
        assert result == [("gameid", "bigint"), ("team", "string")]

    def test_normalize_columns_empty(self):
        """Empty columns list returns empty result."""
        assert _normalize_columns([]) == []

    def test_normalize_columns_missing_keys(self):
        """Missing Name/Type keys handled gracefully."""
        columns = [{"Name": "x"}, {"Type": "int"}]
        result = _normalize_columns(columns)
        assert result == [("x", ""), ("", "int")]
