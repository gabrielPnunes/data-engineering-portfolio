# src/pipeline.py

import time
import traceback
import pandas as pd
from datetime import datetime

import config
from extractor import extract_all_files, check_duplicates, check_date_range
from transform import cast_types, fill_nulls, add_columns, filter_rows, joins
from storage   import load_layer, validate_load
from logger    import log


def _extract(raw_path: str) -> dict:
    dfs = extract_all_files(raw_path, config.FILES)
    check_duplicates(dfs["deliveries"], "delivery_id", "deliveries")
    check_date_range(dfs["deliveries"], "shipped_date", "deliveries")
    return dfs


def _transform(dfs: dict) -> pd.DataFrame:
    df = dfs["deliveries"].copy()
    df = cast_types(df,  config.TRANSFORM_CONFIG["types"])
    df = filter_rows(df, config.TRANSFORM_CONFIG["filters"], "deliveries")
    df = fill_nulls(df,  config.TRANSFORM_CONFIG["fill_nulls"])
    df = add_columns(df, config.TRANSFORM_CONFIG["derived_cols"])

    df_drivers         = dfs["drivers"].copy()
    df_drivers["name"] = df_drivers["name"].str.strip().str.title()
    df_hubs            = dfs["hubs"].copy()
    df_hubs["city"]    = df_hubs["city"].str.strip().str.title()
    df_hubs["state"]   = df_hubs["state"].str.strip().str.upper()

    df = joins(df, df_drivers, on="driver_id", cols=["driver_id", "name"])
    df = joins(df, df_hubs,    on="hub_id",    cols=["hub_id", "city", "state"])
    return df.rename(columns={"name": "driver_name"})


def _load(dfs: dict, df: pd.DataFrame) -> None:
    for name, df_raw in dfs.items():
        load_layer(df_raw, str(config.PROCESSED_PATH.parent), "Data/Processed", f"raw_{name}")

    load_layer(df, str(config.PROCESSED_PATH.parent), "Data/Processed", "deliveries_transformed")

    for query_name, config in config.ANALYTICS_QUERIES.items():
        df_agg = df.groupby(config["group"]).agg(**{
            k: v for k, v in config["metrics"].items()
        }).round(2).reset_index()
        load_layer(df_agg, str(config.ANALYTICS_PATH.parent), "Data/analytics", f"deliveries_{query_name}")


def run() -> None:
    start   = time.time()
    results = {}

    log("pipeline", f"iniciando — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        t0  = time.time()
        dfs = _extract(str(config.RAW_PATH))
        results["extract"] = f"OK ({time.time() - t0:.2f}s)"
        log("extract", results["extract"])

        t0 = time.time()
        df = _transform(dfs)
        results["transform"] = f"OK ({time.time() - t0:.2f}s)"
        log("transform", results["transform"])

        t0 = time.time()
        _load(dfs, df)
        results["load"] = f"OK ({time.time() - t0:.2f}s)"
        log("load", results["load"])

        validate_load(
            str(config.PROCESSED_PATH.parent / "Data" / "Processed" / "deliveries_transformed.parquet"),
            expected_rows=len(df),
            expected_cols=len(df.columns)
        )

    except Exception as e:
        results["error"] = str(e)
        traceback.print_exc()

    log("pipeline", f"tempo total: {time.time() - start:.2f}s")
    for step, status in results.items():
        log(step, status)