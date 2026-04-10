#==BIBLIOTECAS E FUNÇÕES==#
import pandas as pd
from datetime import datetime
from http_client import safe_get
from bcb_client import (
    BCB_BASE_URL, 
    BCB_SERIES, 
    transform_serie)
from json_parser import parse_flat
from storage import (
    save_all_formats, 
    load_parquet)


#==CONFIG==#
OUTPUT_PATH = r"C:\Users\Ceifas\Desktop\data-engineering-portfolio\projects\month01_Fundamentals\Data\Processed"
data_inicial = "01/01/2025"
data_final = datetime.now().strftime("%d/%m/%Y")


INGESTION_CONFIG = {
    "selic": {"date_start": data_inicial, "date_end": data_final},
    "ipca":  {"date_start": data_inicial, "date_end": data_final},
    "usd":   {"date_start": data_inicial, "date_end": data_final},
    "eur":   {"date_start": data_inicial, "date_end": data_final},
}


#==EXTRACT==#
def extract_serie(serie_name: str, date_start: str, date_end: str) -> tuple:
    data_raw = safe_get(
        BCB_BASE_URL.format(codigo=BCB_SERIES[serie_name]),
        params={"formato": "json", "dataInicial": date_start, "dataFinal": date_end}
    )

    if not data_raw:
        print(f"[pipeline] Sem dados para '{serie_name}' - pulando.")
        return None, None

    df = parse_flat(data_raw)
    df = transform_serie(df, serie_name)
    return data_raw, df


#==VALIDATE==#
def validate(df: pd.DataFrame, serie_name: str) -> bool:
    if df is None or df.empty:
        print(f"[validate] '{serie_name}' - DataFrame vazio.")
        return False

    if "date" not in df.columns or serie_name not in df.columns:
        print(f"[validate] '{serie_name}' - colunas esperadas ausentes.")
        return False

    if df[serie_name].isnull().all():
        print(f"[validate] '{serie_name}' - todos os valores são nulos.")
        return False

    print(f"[validate] '{serie_name}' - OK ({len(df)} registros)")
    return True


#==DEA; ESTATISTICAS BASICAS==#
def report(df: pd.DataFrame, serie_name: str) -> None:
    print(f"\n[report] {serie_name.upper()}")
    print(f"  Período:  {df['date'].min()} → {df['date'].max()}")
    print(f"  Registros: {len(df)}")
    print(f"  Mínimo:   {df[serie_name].min():.4f}")
    print(f"  Máximo:   {df[serie_name].max():.4f}")
    print(f"  Média:    {df[serie_name].mean():.4f}")


#==PIPELINE==#
def run_pipeline(config: dict, output_path: str) -> None:

    print("=" * 40)
    print("[pipeline] Iniciando Day 05 - Ingestion Pipeline")
    print("=" * 40)

    results = {}

    for serie_name, params in config.items():
        print(f"\n ===={serie_name.upper()}====" )

        #==EXTRACT==#
        data_raw, df = extract_serie(
            serie_name,
            params["date_start"],
            params["date_end"]
        )

        #==VALIDATE==#
        if not validate(df, serie_name):
            results[serie_name] = "FAILED"
            continue

        #==SAVE==#
        save_all_formats(df, data_raw, output_path, f"{serie_name}_atual")

        #==REPORT==#
        report(df, serie_name)

        results[serie_name] = "OK"

    #==STATUS FINAL==#
    print("\n" + "=" * 40)
    print("[pipeline] Status final:")
    for serie, status in results.items():
        print(f"  {serie.upper()} → {status}")
    print("=" * 40)


#==EXECUÇÃO==#
if __name__ == "__main__":
    run_pipeline(config=INGESTION_CONFIG, output_path=OUTPUT_PATH)