import pandas as pd
from pathlib import Path


def to_bronze(df: pd.DataFrame, base_path: str, filename: str) -> str:
    path = Path(base_path) / "bronze" / f"{filename}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"[bronze] {filename} -> {path} | {len(df)} linhas")
    return str(path)


def to_silver(df: pd.DataFrame, base_path: str, filename: str) -> str:
    path = Path(base_path) / "silver" / f"{filename}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"[silver] {filename} -> {path} | {len(df)} linhas")
    return str(path)


def to_gold(df: pd.DataFrame, base_path: str, filename: str) -> str:
    path = Path(base_path) / "gold" / f"{filename}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"[gold] {filename} -> {path} | {len(df)} linhas")
    return str(path)


def lake_summary(base_path: str) -> pd.DataFrame:
    rows = []
    for layer in ["bronze", "silver", "gold"]:
        layer_path = Path(base_path) / layer
        if not layer_path.exists():
            continue
        for f in sorted(layer_path.rglob("*.parquet")):
            df   = pd.read_parquet(f)
            size = round(f.stat().st_size / 1024, 2)
            rows.append({
                "camada":   layer,
                "arquivo":  f.name,
                "linhas":   len(df),
                "colunas":  len(df.columns),
                "tamanho":  f"{size} KB",
            })
    return pd.DataFrame(rows)

def layer_diff(base_path: str, filename_bronze: str, filename_silver: str) -> None:
    bronze = pd.read_parquet(Path(base_path) / "bronze" / f"{filename_bronze}.parquet")
    silver = pd.read_parquet(Path(base_path) / "silver" / f"{filename_silver}.parquet")
    diff   = len(bronze) - len(silver)
    print(f"[diff] bronze: {len(bronze)} | silver: {len(silver)} | removidas: {diff}")