#==BLIBIOTECAS==#
import pandas as pd
from pathlib import Path
import os

#==FILE PATH==#
path_file = Path("C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.csv")
path_load = Path("C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Staging")
#==EXTRACT==#
def extract(path_file: Path) -> pd.DataFrame:
    extensions = str(path_file).split(".")[-1].lower()

    readers = {
        "csv":      lambda:pd.read_csv(path_file, encoding="latin-1", sep=","),
        "xlsx":     lambda:pd.read_excel(path_file),
        "xls":      lambda:pd.read_excel(path_file),
        "json":     lambda:pd.read_json(path_file),
        "parquet":  lambda:pd.read_parquet(path_file)
    }

    df = readers[extensions]()
    print(f"[extract] {len(df)} linhas extraidas de  '{path_file}'")
    return df


#==TRANSFORM==#
# Normalizar as colunas
def transform_normalize_columns(df: str) -> pd.DataFrame:
    df.columns = (
        df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_", regex = False)
            .str.replace("-", "_", regex = False)
    )

    print(f"[transform, Colunas: {list(df.columns)}")
    return df

# Filtrar valor por coluna
def transform_filter_by_category(df: pd.DataFrame, column: str, value: str) -> pd.DataFrame:
    filtered = df[df[column] == value].copy()
    print(f"[transform] {len(filtered)} linhas com categoria = '{value}'")     
    return filtered

#==LOAD==#
def load(df: pd.DataFrame, output_path: str) -> None:
    os.makedirs(output_path, exist_ok = True)
    path_parquet = output_path / "sales_NY.parquet"
    df.to_parquet(path_parquet, index=False)
    print(f"load Arquivo salvo em '{output_path}'")


#==PIPELINE==# 
def run_pipeline(input_path: Path, column:str, value:str, output_path: Path) -> pd.Dataframe:
    df = extract(path_file)
    df = transform_normalize_columns(df)
    df = transform_filter_by_category(df, column, value)
    load(df, output_path)
    return df


#==EXECUÇÃO==#
if __name__ == "__main__":
    result = run_pipeline(
        input_path=path_file,
        column="city",
        value="New York",
        output_path=path_load
    )
    print(result)