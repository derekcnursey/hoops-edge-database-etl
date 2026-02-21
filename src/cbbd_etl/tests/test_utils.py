"""Tests for utility functions."""

from __future__ import annotations

from cbbd_etl.utils import stable_hash


class TestStableHash:
    def test_stable_hash_deterministic(self):
        """Same input produces the same hash."""
        payload = {"season": 2024, "endpoint": "games"}
        h1 = stable_hash(payload)
        h2 = stable_hash(payload)
        assert h1 == h2

    def test_stable_hash_different_inputs(self):
        """Different inputs produce different hashes."""
        h1 = stable_hash({"season": 2024})
        h2 = stable_hash({"season": 2025})
        assert h1 != h2

    def test_stable_hash_key_order_independent(self):
        """Dict key order does not affect hash."""
        h1 = stable_hash({"a": 1, "b": 2})
        h2 = stable_hash({"b": 2, "a": 1})
        assert h1 == h2

    def test_stable_hash_is_hex_sha256(self):
        """Verify hash looks like a SHA-256 hex digest."""
        h = stable_hash({"key": "value"})
        assert len(h) == 64
        int(h, 16)  # Should not raise for valid hex
