import polars as pl
import time
from pathlib import Path


start_time: float = time.time()

# Pick any two parquet files from the data/ folder.
DATA_DIR: Path = Path(__file__).resolve().parent / "data"
parquet_files: list[Path] = sorted(DATA_DIR.glob("*.parquet"))
if len(parquet_files) < 2:
    raise SystemExit(f"Expected at least 2 parquet files in data/, found {len(parquet_files)}")

df1: pl.DataFrame = pl.read_parquet(parquet_files[0])
df2: pl.DataFrame = pl.read_parquet(parquet_files[1])


ctx = pl.SQLContext(frames={"trip_apr_table": df1, "trip_may_table": df2})

query = """
        SELECT VendorID, Count(VendorID) as cnt
        FROM (
            SELECT VendorID
            FROM trip_apr_table
            UNION ALL
            SELECT VendorID
            FROM trip_may_table
        ) t
        GROUP BY VendorID
"""

print(ctx.execute(query, eager=True))
print(f"Total time taken : {time.time() - start_time}.")