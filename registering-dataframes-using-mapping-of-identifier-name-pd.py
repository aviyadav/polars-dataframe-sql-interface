import pandas as pd
import polars as pl
import time


start_time = time.time()
df1 = pd.read_parquet("data/yellow_tripdata_2022-04.parquet")
df2 = pd.read_parquet("data/yellow_tripdata_2022-05.parquet")

ctx = pl.SQLContext(trip_apr_table=pl.from_pandas(df1), trip_may_table=pl.from_pandas(df2))

query = """ 
            SELECT VendorID, Count(VendorID) as cnt
            FROM   (SELECT VendorID
                    FROM   trip_apr_table
                    UNION ALL
                    SELECT VendorID
                    FROM   trip_may_table) t
            GROUP  BY VendorID 
        """

print(ctx.execute(query,eager=True))
print(f"Total time taken : {time.time() - start_time}.")