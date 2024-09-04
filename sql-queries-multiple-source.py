import polars as pl
import pandas as pd
import time

start_time=time.time()
# pandas dataframe
data = [[1, 'Credit Card'], [2, 'Cash'], [3, 'No Charge'], [4, 'Dispute'], [5, 'Unknown'], [6, 'Voided Trip'],]
df_payment_type = pd.DataFrame(data, columns=['payment_type', 'description'])

with pl.SQLContext(
    trip_data=pl.scan_parquet(["data/yellow_tripdata_2022-04.parquet",
                               "data/yellow_tripdata_2022-05.parquet",
                               "data/yellow_tripdata_2022-06.parquet"]),
    zone_lookup=pl.scan_csv("data/taxi_zone_lookup.csv"),
    payment_type_data=pl.from_pandas(df_payment_type),
    eager=False
) as ctx:
    query = """
        SELECT
            CASE
                WHEN VendorID = 1 THEN 'Creative Mobile Technologies, LLC'
                WHEN VendorID = 2 THEN 'VeriFone Inc.'
                ELSE VendorID
            END as vendor_name,
            pt.description as payment_desc,
            zlpu.Zone as pickuop_zone,
            zldo.Zone as drop_zone,
            passenger_count,
            fare_amount
        FROM
            trip_data td
        LEFT JOIN payment_type_data pt ON pt.payment_type = td.payment_type 
        LEFT JOIN zone_lookup zlpu ON zlpu.LocationID = td.PULocationID
        LEFT JOIN zone_lookup zldo ON zldo.LocationID = td.DOLocationID
        where payment_type in (1,2) and fare_amount>50
        """
    finalDF = ctx.execute(query)
    print(finalDF.limit(5).collect())
    print(f"Total time taken : {time.time() - start_time}.")