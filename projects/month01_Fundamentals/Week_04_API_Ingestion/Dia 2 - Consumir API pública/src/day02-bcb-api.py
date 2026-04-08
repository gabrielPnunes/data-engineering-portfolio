#==BLIBIOTECAS E FUNÇÕES IMPORTADAS==#
import pandas as pd
from datetime import datetime
from bcb_client import (
    extract_by_name,
    transform_serie,
    BCB_SERIES,
)

#==SERIES DISPONIVEIS==#
print("Séries mapeadas:")
for name, codigo in BCB_SERIES.items():
    print(f"  {name} -> {codigo}")

#==CONFIG DE DATA==#
data_inicio = "01/01/2025"
data_fim = datetime.today().strftime("%d/%m/%Y")

#==EXTRAIR SELIC==#
df_selic = extract_by_name("selic", data_inicio, data_fim)
df_selic = transform_serie(df_selic, "selic")
df_selic.head()

#==EXTRAIR IPCA==#
df_ipca = extract_by_name("ipca", data_inicio, data_fim)
df_ipca = transform_serie(df_ipca, "ipca")
df_ipca.head()

#==EXTRAIR USD E REAL==#
df_usd = extract_by_name("usd", data_inicio, data_fim)
df_usd = transform_serie(df_usd, "usd")
df_usd.head()

#==EXTRAIR EUR E REAL==#
df_eur = extract_by_name("eur", data_inicio, data_fim)
df_eur = transform_serie(df_eur, "eur")
df_eur.head()

#==COMPARAR MOEDAS==#
df_merged = pd.merge(df_eur, df_usd, on="date", how="inner")

# Criar relação EUR/USD
df_merged["EUR-USD"] = df_merged["eur"] / df_merged["usd"]

#==DEA BASICA==#
print("SELIC 2025 - ATUAL:")
print(df_selic["selic"].describe())

print("\nUSD 2025 - ATUAL:")
print(df_usd["usd"].describe())

print("\nIPCA 2025 - ATUAL:")
print(df_ipca["ipca"].describe())

print("\nEUR-USD 2025 - ATUAL:")
print(df_merged["EUR-USD"].describe())