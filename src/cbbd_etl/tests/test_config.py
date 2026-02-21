"""Tests for config loading."""

from __future__ import annotations

import os
import textwrap

import pytest

from cbbd_etl.config import Config, load_config, get_api_token


class TestLoadConfigFromYaml:
    def test_load_config_from_yaml(self, tmp_path):
        """Write a minimal config.yaml to tmp, load it, verify all properties."""
        config_content = textwrap.dedent("""\
            bucket: hoops-edge
            region: us-east-1
            api:
              base_url: https://api.collegebasketballdata.com
              timeout_seconds: 30
              max_concurrency: 5
              rate_limit_per_sec: 3
              retry:
                max_attempts: 3
                base_delay_seconds: 0.5
                max_delay_seconds: 8
            seasons:
              start: 2020
              end: 2024
            endpoints:
              games:
                type: season
            s3_layout:
              raw_prefix: raw
              bronze_prefix: bronze
              silver_prefix: silver
              gold_prefix: gold
              ref_prefix: ref
              meta_prefix: meta
              deadletter_prefix: deadletter
              tmp_prefix: tmp
              athena_prefix: athena
        """)
        config_path = tmp_path / "config.yaml"
        config_path.write_text(config_content)

        cfg = load_config(str(config_path))
        assert cfg.bucket == "hoops-edge"
        assert cfg.region == "us-east-1"
        assert cfg.api["base_url"] == "https://api.collegebasketballdata.com"
        assert cfg.api["timeout_seconds"] == 30
        assert cfg.seasons["start"] == 2020
        assert cfg.seasons["end"] == 2024
        assert "games" in cfg.endpoints
        assert cfg.s3_layout["raw_prefix"] == "raw"
        assert cfg.s3_layout["gold_prefix"] == "gold"


class TestLoadConfigWrongBucket:
    def test_load_config_wrong_bucket(self, tmp_path):
        """Verify ValueError when bucket != 'hoops-edge'."""
        config_content = textwrap.dedent("""\
            bucket: wrong-bucket
            api:
              base_url: https://test
            seasons:
              start: 2024
              end: 2024
            endpoints: {}
            s3_layout: {}
        """)
        config_path = tmp_path / "config.yaml"
        config_path.write_text(config_content)

        with pytest.raises(ValueError, match="bucket must be 'hoops-edge'"):
            load_config(str(config_path))


class TestGetApiToken:
    def test_get_api_token_from_env(self, monkeypatch):
        """Set CBBD_API_KEY env var, verify get_api_token() returns it."""
        monkeypatch.setenv("CBBD_API_KEY", "test-token-123")
        monkeypatch.delenv("BEARER_TOKEN", raising=False)
        assert get_api_token() == "test-token-123"

    def test_get_api_token_from_bearer_token(self, monkeypatch):
        """Set BEARER_TOKEN env var, verify get_api_token() returns it."""
        monkeypatch.delenv("CBBD_API_KEY", raising=False)
        monkeypatch.setenv("BEARER_TOKEN", "bearer-token-456")
        assert get_api_token() == "bearer-token-456"

    def test_get_api_token_missing(self, monkeypatch):
        """Verify RuntimeError when no token env vars set."""
        monkeypatch.delenv("CBBD_API_KEY", raising=False)
        monkeypatch.delenv("BEARER_TOKEN", raising=False)
        with pytest.raises(RuntimeError, match="Missing API token"):
            get_api_token()


class TestConfigProperties:
    def test_config_properties(self):
        """Create Config with raw dict, verify all properties work."""
        raw = {
            "bucket": "hoops-edge",
            "region": "us-west-2",
            "api": {"base_url": "https://test.com", "timeout_seconds": 5},
            "seasons": {"start": 2020, "end": 2025},
            "endpoints": {"games": {"type": "season"}},
            "s3_layout": {
                "raw_prefix": "raw",
                "bronze_prefix": "bronze",
                "silver_prefix": "silver",
                "gold_prefix": "gold",
                "ref_prefix": "ref",
                "meta_prefix": "meta",
                "deadletter_prefix": "deadletter",
                "tmp_prefix": "tmp",
                "athena_prefix": "athena",
            },
        }
        cfg = Config(raw)
        assert cfg.bucket == "hoops-edge"
        assert cfg.region == "us-west-2"
        assert cfg.api["base_url"] == "https://test.com"
        assert cfg.seasons["start"] == 2020
        assert cfg.endpoints["games"]["type"] == "season"
        assert cfg.s3_layout["silver_prefix"] == "silver"

    def test_config_default_region(self):
        """Verify region defaults to us-east-1 when not specified."""
        cfg = Config({
            "bucket": "hoops-edge",
            "api": {},
            "seasons": {},
            "endpoints": {},
            "s3_layout": {},
        })
        assert cfg.region == "us-east-1"
