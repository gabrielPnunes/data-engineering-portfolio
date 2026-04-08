#==BLIBIOTECAS E FUNÇÕES==#
import pandas as pd
from http_client import safe_get
from bcb_client import (
    BCB_BASE_URL,
    BCB_SERIES
)
from json_parser import(
    inspect_json,
    parse_flat,
    parse_select_fields,
    parse_normalize
)
from datetime import datetime

#==CONFIGS==#
BASE_URL = BCB_BASE_URL.format(codigo=BCB_SERIES["selic"])

inicial = "01/01/2024"
atual = datetime.now()
atual = atual.strftime("%d/%m/%Y")

print(f"Período: {inicial} -> {atual}")


#==VERIFICAR ESTRTUTURA SELIC==#
print("\n==== Inspecionar estrutura SELIC ====")
data_selic = safe_get(BASE_URL, params={
"formato": "json",
"dataInicial": inicial,
"dataFinal": atual,
})
inspect_json(data_selic)
#DEIXAR A SELIC NO FORMATO CERTO
print("\n==== parse_flat: SELIC ====")
df_selic = parse_flat(data_selic)
print(df_selic.head())


#==DEIXAR APENAS ALGUNS VALORES NO FORMATO==#
print("\n==== parse_select_fields: SELIC ====")
df_selic_slim = parse_select_fields(data_selic, fields=["data", "valor"])
print(df_selic_slim.head())


#==VERIFICAR ESTRTURA DO IPCA==#
print("\n==== Inspecionar estrutura IPCA ====")
BASE_URL_IPCA = BCB_BASE_URL.format(codigo=BCB_SERIES["ipca"])
data_ipca = safe_get(BASE_URL_IPCA, params={
"formato": "json",
"dataInicial": inicial,
"dataFinal": atual,
})
inspect_json(data_ipca)
#NORMALIZAR O IPCA
print("\n==== parse_normalize: IPCA ====")
df_ipca = parse_normalize(data_ipca)
print(df_ipca.head())


#==DEIXAR O USD/BRB NO FORMATO DESEJADO==#
print("\n==== parse_flat: USD/BRL ====")
BASE_URL_USD = BCB_BASE_URL.format(codigo=BCB_SERIES["usd"])
data_usd = safe_get(BASE_URL_USD, params={
"formato": "json",
"dataInicial": inicial,
"dataFinal": atual,
})
df_usd = parse_flat(data_usd)
print(df_usd.head())


#==DEIXAR O EUR/BRL NO FORMATO DESEJADO==#
print("\n=== PARSE SELECT FIELDS: EUR/BRL ===")
data_eur = safe_get(BCB_BASE_URL.format(codigo=BCB_SERIES["eur"]), params={
    "formato":     "json",
    "dataInicial": inicial,
    "dataFinal":   atual,
})
df_eur_slim = parse_select_fields(data_eur, fields=["data", "valor"])
print(df_eur_slim.head())

#==COMPARAR ESTRUTURAS==#
print("\n=== COMPARAR TIPOS DE PARSE ===")
print(f"parse_flat      — selic: {df_selic.shape}")
print(f"parse_slim      — selic: {df_selic_slim.shape}")
print(f"parse_normalize — ipca: {df_ipca.shape}")
print(f"parse_flat      — usd:  {df_usd.shape}")
print(f"parse_slim      — eur:  {df_eur_slim.shape}")