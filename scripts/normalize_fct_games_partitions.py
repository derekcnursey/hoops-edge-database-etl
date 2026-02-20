from __future__ import annotations

import argparse
import os
import subprocess
from typing import List


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize fct_games partition layout to season/asof.")
    parser.add_argument("--bucket", default="hoops-edge")
    parser.add_argument("--season", type=int, default=2024, help="Season to normalize from season/date layout")
    parser.add_argument("--asof", required=True, help="Target asof date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--update-glue", action="store_true", help="Update Glue table to season/asof partitions")
    return parser.parse_args()


def _aws(cmd: List[str]) -> str:
    out = subprocess.check_output(["aws"] + cmd)
    return out.decode("utf-8")


def _update_glue_table() -> None:
    raw = _aws(["glue", "get-table", "--database-name", "cbbd_silver", "--name", "fct_games"])
    table = __import__("json").loads(raw)["Table"]
    cols = table["StorageDescriptor"]["Columns"]
    cols = [c for c in cols if c.get("Name") not in ("season", "asof")]
    table_input = {
        "Name": table["Name"],
        "StorageDescriptor": {
            **table["StorageDescriptor"],
            "Columns": cols,
        },
        "PartitionKeys": [
            {"Name": "season", "Type": "int"},
            {"Name": "asof", "Type": "string"},
        ],
        "TableType": table.get("TableType", "EXTERNAL_TABLE"),
        "Parameters": table.get("Parameters", {}),
    }
    payload = {"DatabaseName": "cbbd_silver", "TableInput": table_input}
    _aws(["glue", "update-table", "--cli-input-json", __import__("json").dumps(payload)])


def main() -> None:
    args = _parse_args()
    bucket = args.bucket
    season = args.season
    asof = args.asof
    dry_run = args.dry_run

    # List date prefixes via aws --query for robustness
    list_cmd = [
        "s3api",
        "list-objects-v2",
        "--bucket",
        bucket,
        "--prefix",
        f"silver/fct_games/season={season}/",
        "--delimiter",
        "/",
        "--query",
        "CommonPrefixes[].Prefix",
        "--output",
        "text",
    ]
    raw = _aws(list_cmd).strip()
    if not raw:
        print("No season/date partitions found to normalize.")
        return
    prefixes = [p for p in raw.split() if "/date=" in p]
    if not prefixes:
        print("No season/date partitions found to normalize.")
        return

    target_prefix = f"s3://{bucket}/silver/fct_games/season={season}/asof={asof}/"
    print(f"Target prefix: {target_prefix}")
    print(f"Found {len(prefixes)} date partitions to copy.")

    for p in prefixes:
        src = f"s3://{bucket}/{p}"
        cmd = ["s3", "cp", "--recursive", src, target_prefix]
        print(" ".join(["aws"] + cmd))
        if not dry_run:
            subprocess.check_call(["aws"] + cmd)

    # Remove old date-based partitions
    for p in prefixes:
        src = f"s3://{bucket}/{p}"
        cmd = ["s3", "rm", "--recursive", src]
        print(" ".join(["aws"] + cmd))
        if not dry_run:
            subprocess.check_call(["aws"] + cmd)

    # Remove stray root asof partition if present
    stray = f"s3://{bucket}/silver/fct_games/asof={asof}/"
    cmd = ["s3", "rm", "--recursive", stray]
    print(" ".join(["aws"] + cmd))
    if not dry_run:
        subprocess.check_call(["aws"] + cmd)

    if args.update_glue and not dry_run:
        _update_glue_table()

    print("Done.")


if __name__ == "__main__":
    main()
