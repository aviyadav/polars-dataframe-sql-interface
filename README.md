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

Rather than hardcoding filenames, the example scripts discover the parquet files
in `data/` at runtime and use the first two (sorted by name). Any two monthly
`yellow_tripdata_*.parquet` files will work, so you can drop in other months —
including ones produced by the generator below — without editing the scripts.

## Generating synthetic data

`generate_tripdata.py` creates a synthetic parquet file that mirrors the schema
and value distributions of the real TLC files, so you can produce additional
months without downloading anything. It takes a year and a month number and
writes `data/yellow_tripdata_<year>-<month>.parquet`:

```bash
python generate_tripdata.py 2022 6
```

Optional flags:

| Flag | Default | Description |
| --- | --- | --- |
| `--rows` | `3600000` | Number of trips to generate |
| `--seed` | `42` | Random seed, for reproducible output |

For example, to generate a small June 2022 file:

```bash
python generate_tripdata.py --year 2022 --month 6 --rows 100000
```

The generated data matches the real files' 19-column schema and dtypes, uses
realistic value frequencies, reproduces the correlated null pattern, and keeps
`total_amount` consistent with the sum of its components.

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

Run any example from anywhere — the scripts resolve the `data/` folder relative
to their own location:

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
