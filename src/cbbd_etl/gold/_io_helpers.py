"""Shared I/O helpers for gold-layer transforms.

Provides convenience functions for reading silver-layer Parquet tables from S3
and common utilities used across gold modules.
"""

from __future__ import annotations

import io
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.compute
import pyarrow.parquet as pq

from ..config import Config
from ..s3_io import S3IO


def read_silver_table(
    s3: S3IO,
    cfg: Config,
    table_name: str,
    season: Optional[int] = None,
    columns: Optional[List[str]] = None,
) -> pa.Table:
    """Read a silver-layer table from S3, optionally filtering by season partition.

    Args:
        s3: S3IO instance for the pipeline bucket.
        cfg: Pipeline configuration.
        table_name: Silver table name (e.g. ``fct_games``, ``dim_teams``).
        season: If provided, restrict to the ``season={season}/`` partition.
        columns: If provided, only read these columns from Parquet files.

    Returns:
        A consolidated ``pyarrow.Table``. Returns an empty table with no columns
        if no data is found.
    """
    silver_prefix = cfg.s3_layout["silver_prefix"]
    if season is not None:
        prefix = f"{silver_prefix}/{table_name}/season={season}/"
    else:
        prefix = f"{silver_prefix}/{table_name}/"

    keys = s3.list_keys(prefix)
    parquet_keys = [k for k in keys if k.endswith(".parquet")]

    if not parquet_keys:
        return pa.table({})

    tables: List[pa.Table] = []
    for key in parquet_keys:
        data = s3.get_object_bytes(key)
        tbl = pq.read_table(io.BytesIO(data), columns=columns)
        tables.append(tbl)

    if not tables:
        return pa.table({})

    # Use permissive promotion to handle type mismatches across files
    # (e.g., int64 vs double, string vs double in optional fields)
    try:
        return pa.concat_tables(tables, promote_options="permissive")
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        # Fall back to casting all tables to a unified schema
        return _concat_with_unified_schema(tables)


def safe_divide(
    numerator: List[Optional[float]],
    denominator: List[Optional[float]],
    scale: float = 1.0,
) -> List[Optional[float]]:
    """Element-wise division with null safety and zero-denominator handling.

    Args:
        numerator: Numerator values.
        denominator: Denominator values.
        scale: Multiply result by this factor (e.g. 100 for percentage).

    Returns:
        List of results, with ``None`` where division is undefined.
    """
    result: List[Optional[float]] = []
    for n, d in zip(numerator, denominator):
        if n is None or d is None or d == 0:
            result.append(None)
        else:
            result.append((n / d) * scale)
    return result


def pydict_get(table: pa.Table, col: str) -> List:
    """Safely extract a column as a Python list, returning empty list if missing."""
    if col in table.column_names:
        return table.column(col).to_pylist()
    return [None] * table.num_rows


def pydict_get_first(table: pa.Table, candidates: List[str]) -> List:
    """Try multiple column names, returning the first found as a Python list."""
    for col in candidates:
        if col in table.column_names:
            return table.column(col).to_pylist()
    return [None] * table.num_rows


def filter_by_season(table: pa.Table, season: int) -> pa.Table:
    """Filter a table by season column (for tables without season partition)."""
    if table.num_rows == 0 or "season" not in table.column_names:
        return table
    mask = pa.compute.equal(table.column("season"), season)
    return table.filter(mask)


def dedup_by(table: pa.Table, key_cols: List[str]) -> pa.Table:
    """Deduplicate a table by key columns, keeping the first occurrence."""
    if table.num_rows == 0:
        return table
    # Verify all key columns exist
    for col in key_cols:
        if col not in table.column_names:
            return table
    seen = set()
    indices = []
    key_data = [table.column(c).to_pylist() for c in key_cols]
    for i in range(table.num_rows):
        key = tuple(key_data[j][i] for j in range(len(key_cols)))
        if key not in seen:
            seen.add(key)
            indices.append(i)
    return table.take(indices)


def table_to_pydict(table: pa.Table) -> Dict[str, List]:
    """Convert a pyarrow Table to a plain Python dict of lists."""
    return {col: table.column(col).to_pylist() for col in table.column_names}


def _concat_with_unified_schema(tables: List[pa.Table]) -> pa.Table:
    """Concatenate tables with incompatible schemas by casting all to string."""
    # Gather all column names and pick the broadest type for each
    col_types: Dict[str, pa.DataType] = {}
    for tbl in tables:
        for field in tbl.schema:
            existing = col_types.get(field.name)
            if existing is None:
                col_types[field.name] = field.type
            elif existing != field.type:
                # If types conflict, promote to the broader type or fall back to string
                col_types[field.name] = _promote_types(existing, field.type)

    unified_schema = pa.schema([(name, typ) for name, typ in col_types.items()])

    aligned: List[pa.Table] = []
    for tbl in tables:
        columns = {}
        for field in unified_schema:
            if field.name in tbl.column_names:
                col = tbl.column(field.name)
                if col.type != field.type:
                    col = col.cast(field.type, safe=False)
                columns[field.name] = col
            else:
                columns[field.name] = pa.nulls(tbl.num_rows, type=field.type)
        aligned.append(pa.table(columns))

    return pa.concat_tables(aligned)


def _promote_types(a: pa.DataType, b: pa.DataType) -> pa.DataType:
    """Choose the broader of two types, falling back to string."""
    numeric = {pa.int8(), pa.int16(), pa.int32(), pa.int64(),
               pa.float16(), pa.float32(), pa.float64()}
    if a in numeric and b in numeric:
        return pa.float64()
    return pa.string()
