# src/s3_pipeline.py

import os
import time
import traceback
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from extractor   import extract_all_files
from transform   import cast_types, fill_nulls, add_columns, filter_rows, joins
from lake        import to_bronze, to_silver, to_gold, lake_summary
from s3_uploader import get_client, upload_folder

load_dotenv()

LAKE_PATH = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month02_Pipeline/Data/lake"
RAW_PATH  = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month02_Pipeline/Data/raw"
BUCKET    = os.getenv("S3_BUCKET",  "de-portfolio-deliveries")
REGION    = os.getenv("AWS_REGION", "us-east-1")

FILES = {
    "deliveries": {
        "file":   "deliveries.csv",
        "schema": ["delivery_id", "driver_id", "hub_id", "status",
                   "shipped_date", "delivered_date", "distance_km",
                   "cost", "customer_rating"],
    },
    "drivers": {"file": "drivers.csv", "schema": ["driver_id", "name", "hub_id"]},
    "hubs":    {"file": "hubs.csv",    "schema": ["hub_id", "city", "state"]},
}

TRANSFORM_CONFIG = {
    "types": {
        "shipped_date":    "datetime",
        "delivered_date":  "datetime",
        "distance_km":     "float",
        "cost":            "float",
        "customer_rating": "float",
    },
    "filters":    {"status": ["delivered"]},
    "fill_nulls": {"customer_rating": 0.0},
    "derived_cols": {
        "delivery_days": lambda d: (d["delivered_date"] - d["shipped_date"]).dt.days,
        "cost_per_km":   lambda d: (d["cost"] / d["distance_km"]).round(2),
    }
}


def extract() -> dict:
    return extract_all_files(RAW_PATH, FILES)


def bronze(dfs: dict) -> None:
    for name, df in dfs.items():
        to_bronze(df, LAKE_PATH, name)


def silver(dfs: dict) -> pd.DataFrame:
    df = dfs["deliveries"].copy()
    df = cast_types(df,  TRANSFORM_CONFIG["types"])
    df = filter_rows(df, TRANSFORM_CONFIG["filters"], "deliveries")
    df = fill_nulls(df,  TRANSFORM_CONFIG["fill_nulls"])
    df = add_columns(df, TRANSFORM_CONFIG["derived_cols"])

    df_drivers         = dfs["drivers"].copy()
    df_drivers["name"] = df_drivers["name"].str.strip().str.title()
    df_hubs            = dfs["hubs"].copy()
    df_hubs["city"]    = df_hubs["city"].str.strip().str.title()
    df_hubs["state"]   = df_hubs["state"].str.strip().str.upper()

    df = joins(df, df_drivers, on="driver_id", cols=["name"])
    df = joins(df, df_hubs,    on="hub_id",    cols=["city", "state"])
    df = df.rename(columns={"name": "driver_name"})

    to_silver(df, LAKE_PATH, "deliveries_clean")
    return df


def gold(df: pd.DataFrame) -> None:
    df_by_hub = df.groupby(["city", "state"]).agg(
        total_deliveries=("delivery_id",     "count"),
        total_revenue   =("cost",            "sum"),
        avg_rating      =("customer_rating", "mean"),
        avg_days        =("delivery_days",   "mean"),
    ).round(2).reset_index()
    to_gold(df_by_hub, LAKE_PATH, "deliveries_by_hub")

    df_by_driver = df.groupby(["driver_id", "driver_name"]).agg(
        total_deliveries=("delivery_id",     "count"),
        total_revenue   =("cost",            "sum"),
        avg_rating      =("customer_rating", "mean"),
    ).round(2).reset_index()
    to_gold(df_by_driver, LAKE_PATH, "deliveries_by_driver")

    df_by_status = df.groupby("status").agg(
        total_deliveries=("delivery_id",  "count"),
        avg_cost        =("cost",         "mean"),
        avg_distance    =("distance_km",  "mean"),
    ).round(2).reset_index()
    to_gold(df_by_status, LAKE_PATH, "deliveries_by_status")


def upload(s3) -> None:
    for layer in ["bronze", "silver", "gold"]:
        upload_folder(s3, f"{LAKE_PATH}/{layer}", BUCKET, layer)


def run() -> None:
    start   = time.time()
    results = {}

    print("=" * 50)
    print(f"[pipeline] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[pipeline] BUCKET: {BUCKET}")
    print(f"[pipeline] LAKE:   {LAKE_PATH}")
    print("=" * 50)

    try:
        s3 = get_client(REGION)

        t0  = time.time()
        dfs = extract()
        results["extract"] = f"OK ({time.time() - t0:.2f}s)"

        t0 = time.time()
        bronze(dfs)
        results["bronze"] = f"OK ({time.time() - t0:.2f}s)"

        t0 = time.time()
        df = silver(dfs)
        results["silver"] = f"OK ({time.time() - t0:.2f}s)"

        t0 = time.time()
        gold(df)
        results["gold"] = f"OK ({time.time() - t0:.2f}s)"

        t0 = time.time()
        upload(s3)
        results["upload"] = f"OK ({time.time() - t0:.2f}s)"

    except Exception as e:
        results["error"] = str(e)
        traceback.print_exc()

    print("\n" + "=" * 50)
    print(f"[pipeline] tempo total: {time.time() - start:.2f}s")
    for step, status in results.items():
        print(f"  {step.upper()} → {status}")
    print("=" * 50)

    print("\n===RESUMO DO LAKE===")
    print(lake_summary(LAKE_PATH).to_string(index=False))