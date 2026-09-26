"""Generate synthetic NYC yellow-taxi trip data.

Produces a parquet file that mirrors the schema and value distributions of the
real ``yellow_tripdata_YYYY-MM.parquet`` files, e.g.::

    python generate_tripdata.py 2022 6
    python generate_tripdata.py --year 2022 --month 6 --rows 100000

The output is written to ``data/yellow_tripdata_<year>-<month>.parquet``.
"""

from __future__ import annotations

import argparse
import calendar
from pathlib import Path

import numpy as np
import polars as pl

DATA_DIR = Path(__file__).resolve().parent / "data"

# Columns that are null together for a small share of rows in the real data.
NULLABLE_COLUMNS = [
    "passenger_count",
    "RatecodeID",
    "store_and_fwd_flag",
    "congestion_surcharge",
    "airport_fee",
]

# Hour-of-day weights: quiet overnight, busy commute and evening hours.
HOUR_WEIGHTS = np.array(
    [2.5, 1.8, 1.4, 0.9, 0.6, 0.8, 1.5, 2.5, 3.5, 3.2, 2.8, 2.8,
     3.0, 3.0, 3.0, 3.1, 3.4, 3.8, 3.9, 3.5, 3.2, 3.2, 3.0, 2.7]
)

# Busy pickup/dropoff zones get a higher weight than the rest of the city.
HOT_ZONES = {
    237: 60, 132: 55, 236: 50, 161: 48, 142: 40, 230: 38, 170: 36,
    138: 30, 79: 28, 234: 26, 186: 24, 43: 22, 148: 20, 249: 20,
    68: 18, 90: 18, 107: 16, 162: 16, 163: 16, 164: 16, 229: 14,
    233: 14, 141: 14, 239: 14, 238: 14, 13: 12, 45: 12, 87: 12,
    88: 12, 231: 12, 232: 12, 261: 10, 125: 10, 158: 10, 211: 10,
    144: 10, 246: 10, 50: 10, 48: 10, 100: 10, 137: 10, 140: 10,
    143: 10, 151: 10, 166: 10, 202: 8, 194: 8, 103: 6, 104: 6,
    105: 6, 264: 4, 265: 4,
}


def weighted_choice(
    rng: np.random.Generator, values: np.ndarray, weights: np.ndarray, size: int
) -> np.ndarray:
    """Draw ``size`` samples from ``values`` using the given (unnormalised) weights."""
    probabilities = np.asarray(weights, dtype=float)
    probabilities = probabilities / probabilities.sum()
    return rng.choice(np.asarray(values), size=size, p=probabilities)


def location_weights() -> np.ndarray:
    """Weights for LocationID 1..265, indexed by location id."""
    weights = np.ones(266, dtype=float)
    weights[0] = 0.0  # LocationID 0 does not exist.
    for location_id, weight in HOT_ZONES.items():
        weights[location_id] = weight
    return weights


def generate(year: int, month: int, rows: int, seed: int) -> pl.DataFrame:
    """Build a synthetic trip dataframe for the given year/month."""
    rng = np.random.default_rng(seed)
    days_in_month = calendar.monthrange(year, month)[1]

    # --- Timestamps -------------------------------------------------------
    start = np.datetime64(f"{year:04d}-{month:02d}-01", "s")
    day = rng.integers(1, days_in_month + 1, size=rows)
    hour = weighted_choice(rng, np.arange(24), HOUR_WEIGHTS, rows)
    minute = rng.integers(0, 60, size=rows)
    second = rng.integers(0, 60, size=rows)
    pickup = (
        start
        + (day - 1).astype("timedelta64[D]")
        + hour.astype("timedelta64[h]")
        + minute.astype("timedelta64[m]")
        + second.astype("timedelta64[s]")
    )

    # --- Trip distance and duration --------------------------------------
    trip_distance = np.round(np.clip(rng.lognormal(0.4, 0.9, rows), 0.01, 60.0), 2)
    duration_seconds = np.clip(
        trip_distance * 150.0 + rng.exponential(300.0, rows), 60.0, None
    ).astype("int64")
    dropoff = pickup + duration_seconds.astype("timedelta64[s]")

    # --- Categorical columns ---------------------------------------------
    vendor_id = weighted_choice(rng, np.array([1, 2, 5, 6]), np.array([0.29, 0.707, 0.00001, 0.002]), rows)
    ratecode_id = weighted_choice(
        rng, np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 99.0]),
        np.array([0.92, 0.037, 0.0027, 0.0013, 0.0066, 0.00001, 0.0032]), rows,
    )
    payment_type = weighted_choice(
        rng, np.array([1, 2, 3, 4]), np.array([0.76, 0.20, 0.0042, 0.0043]), rows,
    )
    store_and_fwd_flag = weighted_choice(
        rng, np.array(["N", "Y"]), np.array([0.984, 0.016]), rows,
    )
    passenger_count = weighted_choice(
        rng, np.arange(10, dtype=float),
        np.array([0.12, 0.55, 0.18, 0.05, 0.03, 0.05, 0.02, 0.005, 0.003, 0.002]), rows,
    )

    loc_weights = location_weights()
    pu_location = weighted_choice(rng, np.arange(266), loc_weights, rows)
    do_location = weighted_choice(rng, np.arange(266), loc_weights, rows)

    # --- Money columns ----------------------------------------------------
    fare_amount = np.round(
        np.clip(3.0 + 2.8 * trip_distance + 0.5 * (duration_seconds / 60.0)
                + rng.normal(0.0, 1.5, rows), 2.5, None), 2,
    )
    extra = weighted_choice(
        rng, np.array([0.0, 0.5, 1.0, 2.5, 3.5]),
        np.array([0.70, 0.10, 0.10, 0.05, 0.05]), rows,
    )
    mta_tax = weighted_choice(rng, np.array([0.5, 0.0]), np.array([0.98, 0.02]), rows)
    improvement_surcharge = weighted_choice(
        rng, np.array([0.3, 0.0]), np.array([0.98, 0.02]), rows,
    )
    congestion_surcharge = weighted_choice(
        rng, np.array([2.5, 2.75, 0.0]), np.array([0.90, 0.05, 0.05]), rows,
    )
    airport_fee = weighted_choice(rng, np.array([1.25, 0.0]), np.array([0.10, 0.90]), rows)
    tolls_amount = np.round(
        np.where(rng.random(rows) < 0.05, rng.choice([6.55, 10.5, 8.0], rows), 0.0), 2,
    )

    # Tips are mostly a card-only phenomenon (~20% of the fare).
    tip_amount = np.round(
        np.where(
            (payment_type == 1) & (rng.random(rows) < 0.9),
            np.clip(fare_amount * rng.normal(0.20, 0.06, rows), 0.0, None),
            0.0,
        ), 2,
    )

    total_amount = np.round(
        fare_amount + extra + mta_tax + tip_amount + tolls_amount
        + improvement_surcharge + congestion_surcharge + airport_fee, 2,
    )

    df = pl.DataFrame(
        {
            "VendorID": vendor_id.astype("int64"),
            "tpep_pickup_datetime": pl.Series(pickup.astype("datetime64[ns]")),
            "tpep_dropoff_datetime": pl.Series(dropoff.astype("datetime64[ns]")),
            "passenger_count": passenger_count,
            "trip_distance": trip_distance,
            "RatecodeID": ratecode_id,
            "store_and_fwd_flag": store_and_fwd_flag,
            "PULocationID": pu_location.astype("int64"),
            "DOLocationID": do_location.astype("int64"),
            "payment_type": payment_type.astype("int64"),
            "fare_amount": fare_amount,
            "extra": extra,
            "mta_tax": mta_tax,
            "tip_amount": tip_amount,
            "tolls_amount": tolls_amount,
            "improvement_surcharge": improvement_surcharge,
            "total_amount": total_amount,
            "congestion_surcharge": congestion_surcharge,
            "airport_fee": airport_fee,
        }
    )

    # A small share of rows carry nulls in the same columns as the real data.
    null_mask = rng.random(rows) < 0.033
    df = df.with_columns(
        pl.when(pl.Series(null_mask)).then(None).otherwise(pl.col(col)).alias(col)
        for col in NULLABLE_COLUMNS
    )
    # Those rows also report payment_type 0 (unknown) in the real data.
    df = df.with_columns(
        pl.when(pl.Series(null_mask)).then(0).otherwise(pl.col("payment_type")).alias("payment_type")
    )
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("year", type=int, nargs="?", help="trip year, e.g. 2022")
    parser.add_argument("month", type=int, nargs="?", help="trip month number, 1-12")
    parser.add_argument("--year", dest="year_flag", type=int, help="trip year, e.g. 2022")
    parser.add_argument("--month", dest="month_flag", type=int, help="trip month number, 1-12")
    parser.add_argument(
        "--rows", type=int, default=3_600_000, help="number of trips to generate"
    )
    parser.add_argument("--seed", type=int, default=42, help="random seed")
    args = parser.parse_args()

    year = args.year if args.year is not None else args.year_flag
    month = args.month if args.month is not None else args.month_flag
    if year is None or month is None:
        parser.error("year and month are required (positionally or via --year/--month)")
    if not 1 <= month <= 12:
        parser.error("month must be between 1 and 12")

    df = generate(year, month, args.rows, args.seed)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output = DATA_DIR / f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    df.write_parquet(output)

    print(f"Wrote {df.height:,} rows to {output}")


if __name__ == "__main__":
    main()
