#==BIBLIOTECAS==#
import pandas as pd
from datetime import datetime
from http_client import safe_get, response_to_dataframe


#==CONFIG==#
BCB_BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"

BCB_SERIES = {
    "selic":  11,
    "ipca":   433,
    "usd":    1,
    "eur":    21619,
}


#==EXTRACT POR SERIE==#
def extract_serie(codigo, data_inicio, data_fim):

    url = BCB_BASE_URL.format(codigo=codigo)
    params = {
        "formato":     "json",
        "dataInicial": data_inicio,
        "dataFinal":   data_fim,
    }

    data = safe_get(url, params=params)

    if not data:
        print(f"[bcb] Sem dados para série {codigo}")
        return pd.DataFrame()

    df = response_to_dataframe(data)
    print(f"[bcb] Série {codigo} — {len(df)} registros ({data_inicio} → {data_fim})")
    return df


#==EXTRACT POR NOME==#
def extract_by_name(name, data_inicio, data_fim):
    if name not in BCB_SERIES:
        raise ValueError(f"[bcb] Série '{name}' não encontrada. Disponíveis: {list(BCB_SERIES.keys())}")

    codigo = BCB_SERIES[name]
    return extract_serie(codigo, data_inicio, data_fim)


#==TRANSFORM==#
def transform_serie(df, serie_name):
    if df.empty:
        return df

    df = df.rename(columns={"data": "date", "valor": serie_name})
    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")
    df[serie_name] = pd.to_numeric(df[serie_name], errors="coerce")
    df = df.sort_values("date").reset_index(drop=True)

    print(f"[transform] '{serie_name}' padronizado — {len(df)} registros")
    return df


"""
#==CONFIG DE DATA==#
data_inicio = "01/01/2025"
data_fim = datetime.today().strftime("%d/%m/%Y")

#==TESTE==#
if __name__ == "__main__":

    print("\n ====SELIC====")
    df_selic = extract_by_name("selic", data_inicio, data_fim)
    df_selic = transform_serie(df_selic, "selic")
    print(df_selic.head())

    print("\n ====IPCA====")
    df_ipca = extract_by_name("ipca", data_inicio, data_fim)
    df_ipca = transform_serie(df_ipca, "ipca")
    print(df_ipca.head())

    print("\n ====USD/BRL====")
    df_usd = extract_by_name("usd", data_inicio, data_fim)
    df_usd = transform_serie(df_usd, "usd")
    print(df_usd.head())

    print("\n ====EUR/BRL====")
    df_eur = extract_by_name("eur", data_inicio, data_fim)
    df_eur = transform_serie(df_eur, "eur")
    print(df_eur.head())
"""