from __future__ import annotations

from typing import List, Optional

import boto3
import pyarrow as pa


class GlueCatalog:
    def __init__(self, region: str) -> None:
        self._client = boto3.client("glue", region_name=region)

    def ensure_database(self, name: str) -> None:
        try:
            self._client.get_database(Name=name)
        except self._client.exceptions.EntityNotFoundException:
            self._client.create_database(DatabaseInput={"Name": name})

    def ensure_table(
        self,
        database: str,
        name: str,
        location: str,
        schema: pa.Schema,
        partition_keys: Optional[List[str]] = None,
    ) -> None:
        columns = [{"Name": f.name, "Type": _pa_to_glue(f.type)} for f in schema]
        partitions = [{"Name": k, "Type": "string"} for k in (partition_keys or [])]
        table_input = {
            "Name": name,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
            "StorageDescriptor": {
                "Columns": columns,
                "Location": location,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
            },
            "PartitionKeys": partitions,
        }
        try:
            existing = self._client.get_table(DatabaseName=database, Name=name)["Table"]
            if _table_matches(existing, table_input):
                return
            self._client.update_table(DatabaseName=database, TableInput=table_input)
        except self._client.exceptions.EntityNotFoundException:
            self._client.create_table(DatabaseName=database, TableInput=table_input)


def _pa_to_glue(dtype: pa.DataType) -> str:
    if pa.types.is_int64(dtype):
        return "bigint"
    if pa.types.is_int32(dtype):
        return "int"
    if pa.types.is_float64(dtype):
        return "double"
    if pa.types.is_boolean(dtype):
        return "boolean"
    if pa.types.is_timestamp(dtype):
        return "timestamp"
    return "string"


def _table_matches(existing: dict, desired: dict) -> bool:
    existing_cols = _normalize_columns(existing.get("StorageDescriptor", {}).get("Columns", []))
    desired_cols = _normalize_columns(desired.get("StorageDescriptor", {}).get("Columns", []))
    if existing_cols != desired_cols:
        return False

    existing_partitions = _normalize_columns(existing.get("PartitionKeys", []))
    desired_partitions = _normalize_columns(desired.get("PartitionKeys", []))
    if existing_partitions != desired_partitions:
        return False

    existing_location = (existing.get("StorageDescriptor", {}).get("Location") or "").rstrip("/")
    desired_location = (desired.get("StorageDescriptor", {}).get("Location") or "").rstrip("/")
    if existing_location != desired_location:
        return False

    return True


def _normalize_columns(columns: List[dict]) -> List[tuple[str, str]]:
    normalized: List[tuple[str, str]] = []
    for col in columns:
        name = str(col.get("Name", "")).lower()
        dtype = str(col.get("Type", "")).lower()
        normalized.append((name, dtype))
    return normalized
