"""Data quality checks — validate data contracts and TABLE_SPECS consistency."""

from __future__ import annotations

import pyarrow as pa
import pytest

from cbbd_etl.normalize import (
    COMMON_TYPE_HINTS,
    TABLE_SPECS,
    normalize_records,
)


class TestAllTableSpecsHavePrimaryKeys:
    def test_all_table_specs_have_primary_keys(self):
        """Every entry in TABLE_SPECS has non-empty primary_keys."""
        for name, spec in TABLE_SPECS.items():
            assert len(spec.primary_keys) > 0, (
                f"TABLE_SPEC '{name}' has empty primary_keys"
            )


class TestTableSpecPrimaryKeysInTypeHints:
    def test_table_spec_primary_keys_in_type_hints(self):
        """For each TableSpec, all primary key fields exist in type_hints
        or in COMMON_TYPE_HINTS (case-insensitive check to account for
        mixed-case conventions in the codebase).
        """
        for name, spec in TABLE_SPECS.items():
            available_hints = {}
            available_hints.update(COMMON_TYPE_HINTS)
            available_hints.update(spec.type_hints)
            # Build a lowercase lookup for case-insensitive matching
            available_lower = {k.lower() for k in available_hints}
            for pk in spec.primary_keys:
                assert pk in available_hints or pk.lower() in available_lower, (
                    f"TABLE_SPEC '{name}': primary key '{pk}' not found "
                    f"in type_hints or COMMON_TYPE_HINTS (even case-insensitive)"
                )


class TestNoDuplicateTableSpecs:
    def test_no_duplicate_table_specs(self):
        """No duplicate names in TABLE_SPECS."""
        names = list(TABLE_SPECS.keys())
        assert len(names) == len(set(names)), "Duplicate TABLE_SPEC names found"


class TestCommonTypeHintsConsistency:
    def test_common_type_hints_consistency(self):
        """All COMMON_TYPE_HINTS map to valid PyArrow types."""
        for field_name, dtype in COMMON_TYPE_HINTS.items():
            assert isinstance(dtype, pa.DataType), (
                f"COMMON_TYPE_HINTS['{field_name}'] is not a valid PyArrow DataType: {dtype}"
            )


class TestGoldTablesRegisteredInSpecs:
    def test_gold_tables_registered_in_specs(self):
        """Every key in GOLD_TRANSFORMS has a corresponding TABLE_SPEC."""
        from cbbd_etl.gold import GOLD_TRANSFORMS

        for name in GOLD_TRANSFORMS:
            assert name in TABLE_SPECS, (
                f"Gold table '{name}' is in GOLD_TRANSFORMS but missing from TABLE_SPECS"
            )


class TestNormalizePreservesPrimaryKeys:
    def test_normalize_preserves_primary_keys(self):
        """For key TABLE_SPECs with sample data,
        normalize and verify primary key columns exist in output.
        """
        # Test a subset of important tables with minimal sample data
        test_cases = {
            "fct_games": [{"gameId": 100, "season": 2024, "homeTeamId": 1, "awayTeamId": 2}],
            "fct_plays": [{"id": 1, "gameId": 100, "period": 1}],
            "dim_teams": [{"teamId": 1, "school": "Duke"}],
            "fct_lines": [{"gameId": 100, "provider": "Bovada"}],
            "fct_game_teams": [{"gameId": 100, "teamId": 1}],
            "fct_rankings": [{"season": 2024, "pollDate": "2024-01-01", "pollType": "AP", "teamId": 1}],
        }

        for table_name, records in test_cases.items():
            spec = TABLE_SPECS[table_name]
            table = normalize_records(table_name, records)
            for pk in spec.primary_keys:
                assert pk in table.column_names, (
                    f"Primary key '{pk}' missing from normalized output for '{table_name}'"
                )


class TestSilverTableSchemasHaveRequiredFields:
    def test_silver_table_schemas_have_required_fields(self):
        """Key silver tables have expected minimum fields."""
        required_fields = {
            "fct_games": ["gameId", "homeTeamId", "awayTeamId"],
            "fct_plays": ["id", "gameId"],
            "fct_lines": ["gameId", "provider"],
            "dim_teams": ["teamId", "school"],
            "fct_game_teams": ["gameId", "teamId"],
            "fct_rankings": ["season", "teamId", "ranking"],
            "fct_ratings_adjusted": ["teamid", "season"],
        }

        for table_name, fields in required_fields.items():
            spec = TABLE_SPECS.get(table_name)
            assert spec is not None, f"TABLE_SPEC missing for '{table_name}'"
            all_hints = {}
            all_hints.update(COMMON_TYPE_HINTS)
            all_hints.update(spec.type_hints)
            for field in fields:
                assert field in all_hints, (
                    f"Required field '{field}' missing from type_hints for '{table_name}'"
                )
