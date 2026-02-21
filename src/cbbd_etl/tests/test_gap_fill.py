"""Tests for gap_fill module."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cbbd_etl.gap_fill import (
    _parse_season_range,
    load_missing_ids_file,
    validate_partitions,
)


def test_parse_season_range_single():
    assert _parse_season_range("2024") == [2024]


def test_parse_season_range_multi():
    assert _parse_season_range("2020-2024") == [2020, 2021, 2022, 2023, 2024]


def test_load_missing_ids_file_basic(tmp_path: Path):
    ids_file = tmp_path / "ids.txt"
    ids_file.write_text("100\n200\n# comment\n300,2024-11-01\n")
    result = load_missing_ids_file(str(ids_file))
    assert len(result) == 3
    assert result[0] == (100, None)
    assert result[1] == (200, None)
    assert result[2] == (300, "2024-11-01")


def test_load_missing_ids_file_empty(tmp_path: Path):
    ids_file = tmp_path / "empty.txt"
    ids_file.write_text("# just comments\n\n")
    result = load_missing_ids_file(str(ids_file))
    assert result == []


def test_load_missing_ids_file_invalid_lines(tmp_path: Path):
    ids_file = tmp_path / "mixed.txt"
    ids_file.write_text("abc\n100\nnot_a_number\n200\n")
    result = load_missing_ids_file(str(ids_file))
    assert len(result) == 2
    assert result[0] == (100, None)
    assert result[1] == (200, None)


def test_validate_partitions_no_issues():
    """Test validate_partitions when S3 returns consistent data."""
    mock_cfg = MagicMock()
    mock_cfg.bucket = "hoops-edge"
    mock_cfg.region = "us-east-1"
    mock_cfg.s3_layout = {"silver_prefix": "silver"}

    with patch("cbbd_etl.gap_fill.S3IO") as MockS3IO:
        mock_s3 = MockS3IO.return_value
        # Return keys for fact tables with consistent asof partitions
        def list_keys_side_effect(prefix):
            if "fct_games" in prefix:
                return [f"{prefix}asof=2024-01-01/part-abc.parquet"]
            if "dim_teams" in prefix:
                return [f"{prefix}asof=2024-01-01/part-abc.parquet"]
            return []

        mock_s3.list_keys.side_effect = list_keys_side_effect
        issues = validate_partitions(mock_cfg, 2024)

        # Most tables will report "No data" since we only mock fct_games and dim_teams
        # But fct_games should not have issues
        assert "fct_games" not in issues


def test_validate_partitions_mixed_patterns():
    """Test that mixed asof/date patterns are flagged."""
    mock_cfg = MagicMock()
    mock_cfg.bucket = "hoops-edge"
    mock_cfg.region = "us-east-1"
    mock_cfg.s3_layout = {"silver_prefix": "silver"}

    with patch("cbbd_etl.gap_fill.S3IO") as MockS3IO:
        mock_s3 = MockS3IO.return_value

        def list_keys_side_effect(prefix):
            if "fct_games/season=2024" in prefix:
                return [
                    "silver/fct_games/season=2024/asof=2024-01-01/part-a.parquet",
                    "silver/fct_games/season=2024/date=2024-01-15/part-b.parquet",
                ]
            return []

        mock_s3.list_keys.side_effect = list_keys_side_effect
        issues = validate_partitions(mock_cfg, 2024)
        assert "fct_games" in issues
        assert any("Mixed partition patterns" in i for i in issues["fct_games"])
