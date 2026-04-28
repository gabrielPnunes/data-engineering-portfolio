from pathlib import Path

BASE_DIR = Path(r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month02_Pipeline")

RAW_PATH       = BASE_DIR / "Data" / "raw"
PROCESSED_PATH = BASE_DIR / "Data" / "processed"
ANALYTICS_PATH = BASE_DIR / "Data" / "analytics"

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
    "filters": {
        "status": ["delivered"]
    },
    "fill_nulls": {
        "customer_rating": 0.0
    },
    "derived_cols": {
        "delivery_days": lambda d: (d["delivered_date"] - d["shipped_date"]).dt.days,
        "cost_per_km":   lambda d: (d["cost"] / d["distance_km"]).round(2),
    }
}

ANALYTICS_QUERIES = {
    "by_hub": {
        "group":   ["city", "state"],
        "metrics": {
            "total_deliveries": ("delivery_id",     "count"),
            "total_revenue":    ("cost",            "sum"),
            "avg_rating":       ("customer_rating", "mean"),
            "avg_days":         ("delivery_days",   "mean"),
        }
    },
    "by_status": {
        "group":   ["status"],
        "metrics": {
            "total_deliveries": ("delivery_id",  "count"),
            "avg_cost":         ("cost",         "mean"),
            "avg_distance":     ("distance_km",  "mean"),
        }
    }
}