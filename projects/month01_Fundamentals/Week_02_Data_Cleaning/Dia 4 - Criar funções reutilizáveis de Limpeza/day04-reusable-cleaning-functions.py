#==BLIBIOTECAS==#
import re
import pandas as pd


#==FUNÇÃO PARA NORMZALIÇÃO DO NOME DAS COLUNAS==#
def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    print(f"[clean] Colunas: {list(df.columns)}")
    return df


#==TRATAMENTO DE VALORES NULOS POR COMPLETO, DROP, E SUBSTITUIÇÃO POR VALOR OU MEDIANA==#
def drop_null_rows(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    before = len(df)
    df = df.dropna(subset=columns)
    print(f"[clean] drop_null_rows: {before - len(df)} linhas removidas")
    return df


def fill_nulls_with_value(df: pd.DataFrame, column: str, value) -> pd.DataFrame:
    count = df[column].isnull().sum()
    df[column] = df[column].fillna(value)
    print(f"[clean] fill_nulls_with_value: {count} nulos em '{column}' → '{value}'")
    return df


def fill_nulls_with_median(df: pd.DataFrame, column: str) -> pd.DataFrame:
    median = df[column].median()
    count = df[column].isnull().sum()
    df[column] = df[column].fillna(median)
    print(f"[clean] fill_nulls_with_median: {count} nulos em '{column}' → mediana={median:.2f}")
    return df


#==PADRONIZAÇÃO DAS STRINGS DE UMA COLUNA==#
def standardize_string_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for col in columns:
        df[col] = df[col].astype(str).str.strip().str.title()
        print(f"[clean] '{col}' → Title Case")
    return df


def standardize_remove_special_chars(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df[column] = df[column].astype(str).apply(lambda x: re.sub(r"[^a-zA-Z0-9\s]", "", x))
    print(f"[clean] Caracteres especiais removidos de '{column}'")
    return df


#==NORMALIZAÇÃO DAS DATAS==#
def standardize_date_column(df: pd.DataFrame, column: str, format: str = "%Y-%m-%d") -> pd.DataFrame:
    df[column] = pd.to_datetime(df[column], infer_datetime_format=True)
    df[column] = df[column].dt.strftime(format)
    print(f"[clean] '{column}' → formato '{format}'")
    return df


#==TRANSFORMA AS COLUNAS PARA O TIPO FLOAT==#   
def standardize_numeric_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for col in columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"[clean] '{col}' → float")
    return df


#==REMOVE VALORES EM OUTLIERS, CONSIDERANDO 1,5 EM CADA EXTREMO==#
def remove_outliers_iqr(df: pd.DataFrame, column: str) -> pd.DataFrame:
    q1 = df[column].quantile(0.25)
    q3 = df[column].quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    before = len(df)
    df = df[(df[column] >= lower) & (df[column] <= upper)]
    print(f"[clean] remove_outliers_iqr: {before - len(df)} outliers removidos de '{column}'")
    return df


#==PIPELINE DE LIMPEZA COMPLETO==#
def clean(df: pd.DataFrame) -> pd.DataFrame:
    print("=" * 40)
    print("[clean] Iniciando limpeza...")

    df = standardize_column_names(df)
    df = drop_null_rows(df, columns=["sales", "order_id"])
    df = fill_nulls_with_value(df, column="postal_code", value="N/A")
    df = standardize_string_columns(df, columns=["category", "region", "segment"])
    df = standardize_date_column(df, column="order_date")
    df = standardize_numeric_columns(df, columns=["sales", "profit", "discount"])
    df = remove_outliers_iqr(df, column="sales")

    print(f"[clean] Concluído. {len(df)} linhas limpas.")
    print("=" * 40)
    return df


#==EXECUÇÃO==#
if __name__ == "__main__":
    from extractor import extract

    df = extract("data/orders.csv")
    df = clean(df)
    print(df.head())