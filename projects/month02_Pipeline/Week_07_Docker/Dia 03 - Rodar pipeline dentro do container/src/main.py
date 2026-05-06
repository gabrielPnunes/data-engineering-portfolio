import os
import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

INPUT_PATH  = Path(os.getenv("INPUT_PATH",  "/data/raw"))
OUTPUT_PATH = Path(os.getenv("OUTPUT_PATH", "/data/processed"))
ENV         = os.getenv("ENV", "development")


def extract(path: Path) -> dict:
    dfs = {}
    for f in path.glob("*.csv"):
        df       = pd.read_csv(f)
        dfs[f.stem] = df
        print(f"[extract] {f.name} — {df.shape[0]} linhas")
    return dfs


def transform(dfs: dict) -> pd.DataFrame:
    df = dfs["deliveries"].copy()
    df = df[df["status"] != "canceled"]
    df = df.dropna(subset=["delivered_date"])
    df["shipped_date"]   = pd.to_datetime(df["shipped_date"],   errors="coerce")
    df["delivered_date"] = pd.to_datetime(df["delivered_date"], errors="coerce")
    df["delivery_days"]  = (df["delivered_date"] - df["shipped_date"]).dt.days
    df["cost_per_km"]    = (df["cost"] / df["distance_km"]).round(2)
    return df


def load(df: pd.DataFrame, path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    out = path / "deliveries_transformed.parquet"
    df.to_parquet(out, index=False)
    print(f"[load] {len(df)} linhas → {out}")


def main():
    print("=" * 40)
    print(f"[pipeline] ENV:    {ENV}")
    print(f"[pipeline] INPUT:  {INPUT_PATH}")
    print(f"[pipeline] OUTPUT: {OUTPUT_PATH}")
    print("=" * 40)

    dfs = extract(INPUT_PATH)
    df  = transform(dfs)
    load(df, OUTPUT_PATH)

    print("\n[pipeline] finalizado")


if __name__ == "__main__":
    main()