"""Shared I/O helpers for gold-layer transforms.

Provides convenience functions for reading silver-layer Parquet tables from S3
and common utilities used across gold modules.
"""

from __future__ import annotations

import io
from typing import Dict, List, Optional

import pyarrow as pa
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

    return pa.concat_tables(tables, promote_options="default")


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


def table_to_pydict(table: pa.Table) -> Dict[str, List]:
    """Convert a pyarrow Table to a plain Python dict of lists."""
    return {col: table.column(col).to_pylist() for col in table.column_names}
