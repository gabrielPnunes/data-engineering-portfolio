import pandas as pd
from extractor import extract_all_files
from transform import cast_types, fill_nulls, add_columns, filter_rows, joins
from storage   import save_csv
from lake      import to_bronze, to_silver, to_gold, lake_summary

RAW_PATH  = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month02_Pipeline/Data/raw"
LAKE_PATH = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month02_Pipeline/Data/lake"

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

print("\n===EXTRACT===")
dfs = extract_all_files(RAW_PATH, FILES)

# BRONZE: dado bruto sem transformação
print("\n===BRONZE===")
for name, df in dfs.items():
    to_bronze(df, LAKE_PATH, name)

# SILVER: dado limpo e padronizado
print("\n===SILVER===")
df = dfs["deliveries"].copy()
df = cast_types(df, {
    "shipped_date":    "datetime",
    "delivered_date":  "datetime",
    "distance_km":     "float",
    "cost":            "float",
    "customer_rating": "float",
})
df = filter_rows(df, {"status": ["delivered"]}, "deliveries")
df = fill_nulls(df, {"customer_rating": 0.0})
df = add_columns(df, {
    "delivery_days": lambda d: (d["delivered_date"] - d["shipped_date"]).dt.days,
    "cost_per_km":   lambda d: (d["cost"] / d["distance_km"]).round(2),
})

df_drivers         = dfs["drivers"].copy()
df_drivers["name"] = df_drivers["name"].str.strip().str.title()
df_hubs            = dfs["hubs"].copy()
df_hubs["city"]    = df_hubs["city"].str.strip().str.title()
df_hubs["state"]   = df_hubs["state"].str.strip().str.upper()

df = joins(df, df_drivers, on="driver_id", cols=["name"])
df = joins(df, df_hubs,    on="hub_id",    cols=["city", "state"])
df = df.rename(columns={"name": "driver_name"})

to_silver(df, LAKE_PATH, "deliveries_clean")

# GOLD: agregações prontas para consumo
print("\n===GOLD===")
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

# RESUMO DO LAKE
print("\n===RESUMO DO LAKE===")
df_summary = lake_summary(LAKE_PATH)
print(df_summary.to_string(index=False))
save_csv(df_summary, f"{LAKE_PATH}/lake_summary.csv")

print("\n[done] Day 01 concluído")