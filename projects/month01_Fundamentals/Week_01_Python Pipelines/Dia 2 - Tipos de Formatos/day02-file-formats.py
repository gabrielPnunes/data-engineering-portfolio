#==BIBLIOTECAS==#
import pandas as pd

#==PATHS==#
path_file_csv = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.csv"
path_file_json = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.json"
path_file_parquet = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.parquet"

#==FUNÇÕES DE LEITURA CSV==#
def read_csv_file(path__file_csv: str):
    return pd.read_csv(path_file_csv)

#==FUNÇÕES DE TRANSFORMAÇÃO PARA JSON E PARQUET==#
def transform_json(df: pd.DataFrame, path: str):
    df.to_json(path, orient="records", lines=True)

def transform_parquet(df: pd.DataFrame, path: str):
    df.to_parquet(path, index=False)

#==FUNÇÕES DE LEITURA JSON E PARQUET==#
def read_json_file(path: str):
    return pd.read_json(path, lines=True)

def read_parquet_file(path: str):
    return pd.read_parquet(path)


#==FUNÇÃO DE EXPLORAÇÃO==#
def explore_data(df: pd.DataFrame):
    print("\nPrimeiras Linhas")
    print(df.head())

    print("\nÚltimas Linhas")
    print(df.tail())

    print("\nColunas")
    print(df.columns)

    print("\nInformações do DF")
    df.info()

    print("\nValores Nulos")
    print(df.isnull().sum())

    print("\nEstrutura do DF (linhas x colunas)")
    print(df.shape)

    print("\nEstatísticas Básicas")
    print(df.describe())

#==MAIN==#
def main():

    print("\nReading CSV...")
    df_csv = read_csv_file(path_file_csv)

    print("\nTransforming JSON...")
    transform_json(df_csv, path_file_csv)

    print("nReading JSON...")
    df_json = read_json_file(path_file_json)

    print("\nTransforming Parquet...")
    transform_parquet(df_csv, path_file_parquet)

    print("Reading Parquet...")
    df_parquet = read_parquet_file(path_file_parquet)

    print("\nFiles generated successfully.")

    print("E")
    explore_data(df_csv)

#==EXECUÇÃO==#
if __name__ == "__main__":
    main()