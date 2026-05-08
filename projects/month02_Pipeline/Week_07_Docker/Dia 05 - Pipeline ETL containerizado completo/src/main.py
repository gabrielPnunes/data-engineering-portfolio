import os
import time
import traceback
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from extractor import extract_all_files
from transform import cast_types, fill_nulls, add_columns, filter_rows, joins
from storage   import save_parquet

load_dotenv()

RAW       = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month02_Pipeline/Data/raw"
PROCESSED = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month02_Pipeline/Data/Processed"

INPUT_PATH  = Path(os.getenv("INPUT_PATH",  RAW))
OUTPUT_PATH = Path(os.getenv("OUTPUT_PATH", PROCESSED))

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "pipeline")
DB_USER = os.getenv("DB_USER", "pipeline")
DB_PASS = os.getenv("DB_PASS", "pipeline123")
DB_URL  = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

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


def extract():
    return extract_all_files(str(INPUT_PATH), FILES)


def transform(dfs):
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
    return df.rename(columns={"name": "driver_name"})


def load(df):
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    save_parquet(df, str(OUTPUT_PATH / "deliveries_transformed.parquet"))

    try:
        engine = create_engine(DB_URL)
        with engine.connect() as con:
            con.execute(text("SELECT 1"))
            print(f"[db] conectado — {DB_HOST}:{DB_PORT}/{DB_NAME}")
        df.to_sql("deliveries", engine, if_exists="replace", index=False)
        print(f"[db] {len(df)} linhas → tabela 'deliveries'")
    except Exception as e:
        print(f"[db] banco não disponível localmente — pulando: {e}")


def run():
    start   = time.time()
    results = {}

    print("=" * 50)
    print(f"[pipeline] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[pipeline] ENV:    {os.getenv('ENV', 'development')}")
    print(f"[pipeline] INPUT:  {INPUT_PATH}")
    print(f"[pipeline] OUTPUT: {OUTPUT_PATH}")
    print("=" * 50)

    try:
        t0 = time.time()
        dfs = extract()
        results["extract"] = f"OK ({time.time() - t0:.2f}s)"
        print(f"[extract] {results['extract']}")

        t0 = time.time()
        df = transform(dfs)
        results["transform"] = f"OK ({time.time() - t0:.2f}s)"
        print(f"[transform] {results['transform']}")

        t0 = time.time()
        load(df)
        results["load"] = f"OK ({time.time() - t0:.2f}s)"
        print(f"[load] {results['load']}")

    except Exception as e:
        results["error"] = str(e)
        traceback.print_exc()

    print("\n" + "=" * 50)
    print(f"[pipeline] tempo total: {time.time() - start:.2f}s")
    for step, status in results.items():
        print(f"  {step.upper()} → {status}")
    print("=" * 50)


if __name__ == "__main__":
    run()