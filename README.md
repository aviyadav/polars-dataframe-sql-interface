# polars-dataframe-sql-interface

A collection of small, self-contained examples showing how to query
[Polars](https://pola.rs/) DataFrames and LazyFrames with SQL using
`pl.SQLContext`. The scripts demonstrate the different ways of registering
frames, controlling eager vs. lazy execution, and joining multiple data
sources in a single query.

### Source Article
https://bigdataenthusiast.medium.com/polars-dataframe-sql-interface-d96652d8c358

## Requirements

- Python `>= 3.13`
- [`polars`](https://pypi.org/project/polars/)
- [`pandas`](https://pypi.org/project/pandas/) (only for the pandas interop example)
- [`pyarrow`](https://pypi.org/project/pyarrow/)

Dependencies are declared in `pyproject.toml` and pinned in `requirements.txt`.
Install them with either:

```bash
pip install -r requirements.txt
```

or, if you use [uv](https://docs.astral.sh/uv/):

```bash
uv sync
```

## Data

The examples read NYC TLC yellow taxi trip records from the `data/` folder:

| File | Description |
| --- | --- |
| `data/yellow_tripdata_2022-04.parquet` | April 2022 trips |
| `data/yellow_tripdata_2022-05.parquet` | May 2022 trips |
| `data/taxi_zone_lookup.csv` | Taxi zone `LocationID` → `Zone` lookup |

Data courtesy of the NYC Taxi & Limousine Commission:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

> Note: `sql-queries-multiple-source.py` also references
> `data/yellow_tripdata_2022-06.parquet`, which is not included in this repo.
> Download it from the TLC link above or remove it from the `scan_parquet` list
> before running that script.

## Scripts

Each script runs the same core query — a `UNION ALL` of two monthly trip tables
grouped by `VendorID` — but varies how the SQL context is configured.

| Script | What it demonstrates |
| --- | --- |
| `registering-dataframes-using-mapping-of-identifier-name-pl.py` | Registering frames via a `{name: frame}` mapping |
| `registering-dataframes-using-mapping-of-identifier-name-pl-eager-true.py` | `SQLContext(eager_execution=True)` |
| `registering-dataframes-using-mapping-of-identifier-name-pl-eager-false.py` | `SQLContext(eager_execution=False)` with `.collect()` |
| `registering-dataframes-using-mapping-of-identifier-name-pl-eager-plan.py` | Lazy execution returning a query plan |
| `registering-dataframes-using-mapping-of-identifier-name-pl-lazy.py` | Lazy execution with commented-out alternatives |
| `registering-dataframes-using-mapping-of-identifier-name-pd.py` | Converting pandas DataFrames with `pl.from_pandas` before querying |
| `sql-queries-multiple-source.py` | Joining Parquet trips with a CSV zone lookup and a pandas payment-type table |

## Usage

Run any example from the project root so the relative `data/...` paths resolve:

```bash
python registering-dataframes-using-mapping-of-identifier-name-pl.py
```

Example output:

```
shape: (2, 2)
┌──────────┬────────┐
│ VendorID ┆ cnt    │
╞══════════╪════════╡
│ 1        ┆ ...    │
│ 2        ┆ ...    │
└──────────┴────────┘
Total time taken : ... .
```

## References & Links

- Polars SQL interface: https://docs.pola.rs/user-guide/sql/
- Polars SQL keywords: https://docs.rs/polars-sql/latest/src/polars_sql/keywords.rs.html
- NYC TLC trip record data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
