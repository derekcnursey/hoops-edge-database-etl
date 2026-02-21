"""Extended normalize tests — supplements existing test_normalize.py."""

from __future__ import annotations

import pyarrow as pa
import pytest

from cbbd_etl.normalize import (
    TABLE_SPECS,
    _cast_value,
    _infer_type,
    dedupe_records,
    normalize_records,
)


class TestNormalizeRecordsUnknownTable:
    def test_normalize_records_unknown_table(self):
        """Verify graceful handling for table not in TABLE_SPECS."""
        records = [{"x": 1, "y": "hello"}]
        table = normalize_records("unknown_table_xyz", records)
        assert table.num_rows == 1
        # x should be inferred as int64, y as string
        assert table.column("x").to_pylist() == [1]
        assert table.column("y").to_pylist() == ["hello"]


class TestNormalizeRecordsNullValues:
    def test_normalize_records_null_values(self):
        """Verify None handling across all types."""
        records = [
            {
                "gameId": None,
                "season": None,
                "team": None,
                "score": None,
            }
        ]
        table = normalize_records("fct_games", records)
        assert table.num_rows == 1
        # gameId should be None (int64 type hint from TABLE_SPECS)
        assert table.column("gameId").to_pylist() == [None]
        # season should be None (int32 from COMMON_TYPE_HINTS)
        assert table.column("season").to_pylist() == [None]


class TestNormalizeRecordsEmptyList:
    def test_normalize_records_empty_list(self):
        """Verify empty input returns 0-row table."""
        table = normalize_records("fct_games", [])
        assert table.num_rows == 0


class TestCastValueEdgeCases:
    def test_cast_value_bool_true_string(self):
        """String 'true' casts to True for boolean type."""
        assert _cast_value("true", pa.bool_()) is True

    def test_cast_value_bool_false_string(self):
        """String 'false' casts to False for boolean type."""
        assert _cast_value("false", pa.bool_()) is False

    def test_cast_value_bool_yes_string(self):
        """String 'yes' casts to True for boolean type."""
        assert _cast_value("yes", pa.bool_()) is True

    def test_cast_value_bool_1_string(self):
        """String '1' casts to True for boolean type."""
        assert _cast_value("1", pa.bool_()) is True

    def test_cast_value_bool_no_string(self):
        """String 'no' casts to False for boolean type."""
        assert _cast_value("no", pa.bool_()) is False

    def test_cast_value_invalid_int(self):
        """Non-numeric string returns None for int type."""
        assert _cast_value("not_a_number", pa.int64()) is None

    def test_cast_value_none(self):
        """None input returns None for all types."""
        assert _cast_value(None, pa.int64()) is None
        assert _cast_value(None, pa.float64()) is None
        assert _cast_value(None, pa.string()) is None
        assert _cast_value(None, pa.bool_()) is None

    def test_cast_value_float_string(self):
        """Float string casts correctly."""
        assert _cast_value("3.14", pa.float64()) == pytest.approx(3.14)

    def test_cast_value_invalid_float(self):
        """Non-numeric string returns None for float type."""
        assert _cast_value("not_a_float", pa.float64()) is None

    def test_cast_value_int_from_float_string(self):
        """Float string '3.14' cast to int returns None (int() rejects float strings)."""
        # Python's int("3.14") raises ValueError, so _cast_value returns None
        assert _cast_value("3.14", pa.int64()) is None

    def test_cast_value_int_from_int_string(self):
        """Integer string '42' cast to int returns 42."""
        assert _cast_value("42", pa.int64()) == 42

    def test_cast_value_timestamp(self):
        """ISO datetime string casts to datetime."""
        result = _cast_value("2024-01-15T12:00:00Z", pa.timestamp("s"))
        assert result is not None
        assert result.year == 2024
        assert result.month == 1

    def test_cast_value_invalid_timestamp(self):
        """Invalid timestamp string returns None."""
        result = _cast_value("not-a-date", pa.timestamp("s"))
        assert result is None

    def test_cast_value_string_passthrough(self):
        """Non-string values get cast to string."""
        assert _cast_value(42, pa.string()) == "42"
        assert _cast_value(True, pa.string()) == "True"


class TestInferType:
    def test_infer_type_bool(self):
        """Bool value infers as pa.bool_()."""
        assert _infer_type(True) == pa.bool_()
        assert _infer_type(False) == pa.bool_()

    def test_infer_type_int(self):
        """Int value infers as pa.int64()."""
        assert _infer_type(42) == pa.int64()

    def test_infer_type_float(self):
        """Float value infers as pa.float64()."""
        assert _infer_type(3.14) == pa.float64()

    def test_infer_type_string(self):
        """String value infers as pa.string()."""
        assert _infer_type("hello") == pa.string()

    def test_infer_type_none(self):
        """None value infers as pa.string() (default)."""
        assert _infer_type(None) == pa.string()


class TestDedupeEmptyKeys:
    def test_dedupe_empty_keys(self):
        """Verify dedupe_records with empty key tuple returns all records."""
        records = [
            {"id": 1, "name": "a"},
            {"id": 1, "name": "b"},
            {"id": 2, "name": "c"},
        ]
        result = dedupe_records(records, ())
        assert len(result) == 3
