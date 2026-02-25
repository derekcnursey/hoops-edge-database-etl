"""Quick check of season 2024 gold layer for preseason prior diagnostics."""
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from cbbd_etl.config import load_config
from cbbd_etl.s3_io import S3IO
import pyarrow.parquet as pq

cfg = load_config(str(Path(__file__).resolve().parent.parent / "config.yaml"))
s3 = S3IO(cfg.bucket, cfg.region)

prefix = "gold/team_adjusted_efficiencies_no_garbage/season=2024/"
keys = s3.list_keys(prefix)
parquet_keys = [k for k in keys if k.endswith(".parquet")]
print(f"Found {len(parquet_keys)} parquet files for season 2024")

if not parquet_keys:
    print("No data!")
    sys.exit(1)

# Just read the first file (there should only be 1-2 asof partitions)
data = s3.get_object_bytes(parquet_keys[0])
tbl = pq.read_table(io.BytesIO(data), columns=["teamId", "rating_date", "adj_oe", "adj_de"])
print(f"Rows: {tbl.num_rows}")

team_ids = tbl.column("teamId").to_pylist()
dates = tbl.column("rating_date").to_pylist()
adj_oes = tbl.column("adj_oe").to_pylist()
adj_des = tbl.column("adj_de").to_pylist()

# Find latest per team
latest = {}
for i in range(len(team_ids)):
    tid = team_ids[i]
    dt = str(dates[i])[:10] if dates[i] is not None else ""
    oe = adj_oes[i]
    de = adj_des[i]
    if tid is None or oe is None or de is None:
        continue
    tid = int(tid)
    if tid not in latest or dt > latest[tid][0]:
        latest[tid] = (dt, float(oe), float(de))

print(f"Teams with final ratings: {len(latest)}")

final_oes = [v[1] for v in latest.values()]
final_des = [v[2] for v in latest.values()]
import numpy as np
print(f"Final adj_oe: mean={np.mean(final_oes):.2f}, std={np.std(final_oes):.2f}, min={np.min(final_oes):.2f}, max={np.max(final_oes):.2f}")
print(f"Final adj_de: mean={np.mean(final_des):.2f}, std={np.std(final_des):.2f}, min={np.min(final_des):.2f}, max={np.max(final_des):.2f}")

# Show date range
all_dates = sorted(set(str(d)[:10] for d in dates if d))
print(f"Date range: {all_dates[0]} to {all_dates[-1]} ({len(all_dates)} dates)")
