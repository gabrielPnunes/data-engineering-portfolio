#==BIBLIOTECAS==#
import pandas as pd
from datetime import datetime
from http_client import safe_get
from bcb_client import BCB_BASE_URL, BCB_SERIES, transform_serie
from json_parser import parse_flat
from storage import (
    save_all_formats,
    load_json,
    load_csv,
    load_parquet,
)

#==CONFIG==#
OUTPUT_PATH = r"C:\Users\Ceifas\Desktop\data-engineering-portfolio\projects\month01_Fundamentals\Data\Processed"
data_inicial = "01/01/2025"
data_final = datetime.now().strftime("%d/%m/%Y")


#==EXTRACT==#
def extract_serie(serie_name: str) -> tuple:
    data_raw = safe_get(
        BCB_BASE_URL.format(codigo=BCB_SERIES[serie_name]),
        params={"formato": "json", "dataInicial": data_inicial, "dataFinal": data_final}
    )
    df = parse_flat(data_raw)
    df = transform_serie(df, serie_name)
    return data_raw, df

print("[pipeline] Iniciando Day 04 - Save Data...")


#==SALVANDO SELIC==#
print("\n==== SELIC ====")
data_raw, df_selic = extract_serie("selic")
save_all_formats(df_selic, data_raw, OUTPUT_PATH, "selic_atual")

#==SALVANDO IPCA==#
print("\n==== IPCA ====")
data_raw, df_ipca = extract_serie("ipca")
save_all_formats(df_ipca, data_raw, OUTPUT_PATH, "ipca_atual")

#==SALVANDO USD==#
print("\n==== USD/BRL ====")
data_raw, df_usd = extract_serie("usd")
save_all_formats(df_usd, data_raw, OUTPUT_PATH, "usd_atual")

#==SALVANDO EUR==#
print("\n==== EUR/BRL ====")
data_raw, df_eur = extract_serie("eur")
save_all_formats(df_eur, data_raw, OUTPUT_PATH, "eur_atual")

#==VERIFICAÇÃO==#
print("\n==== Verificando arquivos salvos ====")

df_selic_check = load_parquet(f"{OUTPUT_PATH}/parquet/selic_atual.parquet")
print(df_selic_check.head())

df_ipca_check = load_csv(f"{OUTPUT_PATH}/csv/ipca_atual.csv")
print(df_ipca_check.head())

data_usd_check = load_json(f"{OUTPUT_PATH}/raw/usd_atual.json")
print(f"JSON carregado — {len(data_usd_check)} registros")

df_eur_check = load_parquet(f"{OUTPUT_PATH}/parquet/eur_atual.parquet")
print(df_eur_check.head())