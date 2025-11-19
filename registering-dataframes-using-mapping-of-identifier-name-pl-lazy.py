import polars as pl
import time


start_time: float = time.time()
df1: pl.DataFrame = pl.read_parquet("data/yellow_tripdata_2022-04.parquet")
df2: pl.DataFrame = pl.read_parquet("data/yellow_tripdata_2022-05.parquet")

# ctx = pl.SQLContext( eager_execution=True, frames={"trip_apr_table": df1, "trip_may_table": df2})
ctx: pl.SQLContext = pl.SQLContext( eager_execution=False, frames={"trip_apr_table": df1, "trip_may_table": df2})
# ctx = pl.SQLContext(frames={"trip_apr_table": df1, "trip_may_table": df2})
query: str = """
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

# print(ctx.execute(query).collect())
print(ctx.execute(query))
# print(ctx.execute(query, eager=True))
print(f"Total time taken : {time.time() - start_time}.")