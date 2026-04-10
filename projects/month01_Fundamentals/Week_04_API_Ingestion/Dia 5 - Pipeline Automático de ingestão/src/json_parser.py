#==BIBLIOTECAS==#
import pandas as pd

#==INSPEÇÃO==#
def inspect_json(data, level: int = 0, max_level: int = 3) -> None:
    indent = "  " * level

    if level > max_level:
        print(f"{indent}...")
        return

    if isinstance(data, dict):
        for key, value in data.items():
            print(f"{indent}[dict] '{key}': {type(value).__name__}")
            if isinstance(value, (dict, list)):
                inspect_json(value, level + 1, max_level)

    elif isinstance(data, list):
        print(f"{indent}[list] {len(data)} itens → {type(data[0]).__name__ if data else 'vazio'}")
        if data:
            inspect_json(data[0], level + 1, max_level)

    else:
        print(f"{indent}[value] {data}")


#==PARSING==#
def parse_flat(data: list) -> pd.DataFrame:
    df = pd.DataFrame(data)
    print(f"[parse] parse_flat — {len(df)} linhas, {len(df.columns)} colunas")
    return df


def parse_nested(data: dict, record_path: str) -> pd.DataFrame:
    if record_path not in data:
        raise KeyError(f"[parse] Chave '{record_path}' não encontrada no JSON")

    records = data[record_path]
    df = pd.DataFrame(records)
    print(f"[parse] parse_nested['{record_path}'] — {len(df)} linhas")
    return df


def parse_normalize(data, record_path: list = None, meta: list = None) -> pd.DataFrame:
    df = pd.json_normalize(data, record_path=record_path, meta=meta)
    print(f"[parse] parse_normalize — {len(df)} linhas, {len(df.columns)} colunas")
    return df


def parse_select_fields(data: list, fields: list) -> pd.DataFrame:
    records = [{field: item.get(field) for field in fields} for item in data]
    df = pd.DataFrame(records)
    print(f"[parse] parse_select_fields — campos: {fields}")
    return df


#==LIMPEZA PÓS-PARSE==#
def flatten_column(df: pd.DataFrame, column: str, prefix: str = None) -> pd.DataFrame:
    prefix = prefix or column
    normalized = pd.json_normalize(df[column].tolist())
    normalized.columns = [f"{prefix}_{col}" for col in normalized.columns]
    df = df.drop(columns=[column]).reset_index(drop=True)
    df = pd.concat([df, normalized], axis=1)
    print(f"[parse] flatten_column '{column}' → {list(normalized.columns)}")
    return df


#==TESTE==#
"""

if __name__ == "__main__":
    BASE_URL = BCB_BASE_URL.format(codigo=BCB_SERIES["selic"])

    print("\n ====Inspecionar estrutura SELIC==== ")
    data_selic = safe_get(BASE_URL, params={
        "formato": "json",
        "dataInicial": "01/01/2024",
        "dataFinal": "31/01/2024",
    })
    inspect_json(data_selic)

    print("\n ====parse_flat: SELIC==== ")
    df_selic = parse_flat(data_selic)
    print(df_selic.head())

    print("\n ====parse_select_fields: SELIC==== ")
    df_selic_slim = parse_select_fields(data_selic, fields=["data", "valor"])
    print(df_selic_slim.head())

    print("\n ====Inspecionar estrutura IPCA==== ")
    BASE_URL_IPCA = BCB_BASE_URL.format(codigo=BCB_SERIES["ipca"])
    data_ipca = safe_get(BASE_URL_IPCA, params={
        "formato": "json",
        "dataInicial": "01/01/2024",
        "dataFinal": "31/12/2024",
    })
    inspect_json(data_ipca)

    print("\n ====parse_normalize: IPCA==== ")
    df_ipca = parse_normalize(data_ipca)
    print(df_ipca.head())

    print("\n ====parse_flat: USD/BRL==== ")
    BASE_URL_USD = BCB_BASE_URL.format(codigo=BCB_SERIES["usd"])
    data_usd = safe_get(BASE_URL_USD, params={
        "formato": "json",
        "dataInicial": "01/01/2024",
        "dataFinal": "31/12/2024",
    })
    df_usd = parse_flat(data_usd)
    print(df_usd.head())

"""