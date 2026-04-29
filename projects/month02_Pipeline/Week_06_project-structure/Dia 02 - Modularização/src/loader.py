import json
import os
import pandas as pd
from pathlib import Path


def save_json(data, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[storage] JSON salvo em '{output_path}'")


def save_csv(df: pd.DataFrame, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"[storage] CSV salvo em '{output_path}' - {len(df)} linhas")


def save_parquet(df: pd.DataFrame, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"[storage] Parquet salvo em '{output_path}' - {len(df)} linhas")


def save_all_formats(df: pd.DataFrame, data_raw, base_path: str, filename: str) -> None:
    save_json(data_raw, os.path.join(base_path, "raw",     f"{filename}.json"))
    save_csv(df,        os.path.join(base_path, "csv",     f"{filename}.csv"))
    save_parquet(df,    os.path.join(base_path, "parquet", f"{filename}.parquet"))
    print(f"[storage] '{filename}' salvo em todos os formatos.")


def load_json(filepath: str) -> dict:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"[storage] JSON carregado de '{filepath}'")
    return data


def load_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, encoding="utf-8")
    print(f"[storage] CSV carregado de '{filepath}' - {len(df)} linhas")
    return df


def load_parquet(filepath: str) -> pd.DataFrame:
    df = pd.read_parquet(filepath)
    print(f"[storage] Parquet carregado de '{filepath}' - {len(df)} linhas")
    return df


def load_layer(df: pd.DataFrame, base_path: str, layer: str, filename: str) -> None:
    save_parquet(df, f"{base_path}\\{layer}\\parquet\\{filename}.parquet")
    save_csv(df,     f"{base_path}\\{layer}\\csv\\{filename}.csv")
    print(f"[load] '{filename}' -> camada '{layer}'")


def validate_load(path: str, expected_rows: int, expected_cols: int) -> bool:
    df = load_parquet(path)
    ok = df.shape == (expected_rows, expected_cols)
    print(f"[validate] {Path(path).name} - esperado ({expected_rows}, {expected_cols}) | encontrado {df.shape} | {'OK' if ok else 'FALHOU'}")
    return ok


def load_summary(layers: dict) -> pd.DataFrame:
    rows = []
    for layer, paths in layers.items():
        for path in paths:
            p    = Path(path)
            size = round(p.stat().st_size / 1024, 2) if p.exists() else 0
            df   = load_parquet(str(p)) if p.exists() else pd.DataFrame()
            rows.append({
                "camada":  layer,
                "arquivo": p.name,
                "linhas":  len(df),
                "colunas": len(df.columns),
                "tamanho": f"{size} KB",
            })
    return pd.DataFrame(rows)