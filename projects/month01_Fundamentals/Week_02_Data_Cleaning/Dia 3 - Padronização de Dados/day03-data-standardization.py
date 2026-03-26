#==BLIBIOTECAS==#
import pandas as pd


#==FUNÇÕES PARA PADRONIZAÇÃO DE STRINGS==#
def standardize_to_tile(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for col in columns:
        df[col] = df[col].astype(str).str.strip().str.title()
    print(f"[standardize] '{col}' padronizado para Title Case")
    return df

def standardize_to_uppercase(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for col in columns:
        df[col] = df[col].astype(str).str.strip().str.upper()
    print(f"[standardize] '{col}' padronizado para UPPERCASE")
    return df

def standardize_to_lowercase(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for col in columns:
        df[col] = df[col].astype(str).str.strip().str.lower()
    print(f"[standardize] '{col}' padronizado para lowercase")
    return df


#==FUNÇÕES PARA PADRONIZAÇÃO DE DATA==#
def standardize_date_column(df: pd.DataFrame, columns: list, format: str = "%Y-%m-%d") -> pd.DataFrame:
    df[columns] = pd.to_datetime(df[columns], infer_datetime_format = True)
    df[columns] = df[columns].dt.strftime(format)
    print(f"[standardize] '{columns}' convertido para formato '{format}'")
    return df

#==FUNÇÕES PARA CONVERSÃO DE TIPOS==#
def standardize_numeric_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for col in columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"[standardize] '{col}' convertido para float")
    return df

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    print(f"[standardize] Colunas: {list(df.columns)}")
    return df


#==EXECUÇÃO==#
if __name__ == "__main__":
    from src.extractor import extract

    df = extract("C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.parquet")

    df = standardize_column_names(df)

    df = standardize_to_tile(df, columns=["customer_type", "gender", "product_category"])
    #df = standardize_date_column(df, column="order_date", format="%Y-%m-%d")
    df = standardize_numeric_columns(df, columns=["quantity", "tax", "reward_points"])

    print(f"\n[result] {len(df)} linhas padronizadas")

print(df.head())


