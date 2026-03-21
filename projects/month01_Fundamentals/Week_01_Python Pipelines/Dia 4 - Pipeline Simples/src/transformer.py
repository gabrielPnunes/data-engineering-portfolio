#==BIBLIOTECAS==#
import pandas as pd

#==FUNÇÃO TRANSFORMER==#
# normalizar colunas no padrão 
def transform_normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    print(f"[transform] Colunas normalizadas: {list(df.columns)}")
    return df

# filtrar por coluna valor
def transform_filtred_by_column_value(df: pd.DataFrame, column: str, value: str) -> pd.DataFrame:
    filtered = df[df[column] == value].copy()
    print(f"[transform] {len(filtered)} linhas com valor '{value}' em '{column}'")
    return filtered

# filtrar apenas colunas selecionadas
def transform_select_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    df = df[columns].copy()
    print(f"[transform] Colunas selecionadas: {columns}")
    return df