#==BIBLIOTECAS==#
import pandas as pd

#==PATHS==#
path_file_csv = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.csv"
path_file_json = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.json"
path_file_parquet = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.parquet"

#==FUNÇÕES DE LEITURA==#
def read_csv_file(path: str):
    return pd.read_csv(path)

#==FUNÇÕES DE TRANSFORMAÇÃO==#
def transform_json(df: pd.DataFrame, path: str):
    df.to_json(path, orient="records", lines=True)

def transform_parquet(df: pd.DataFrame, path: str):
    df.to_parquet(path, index=False)

#########
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
    df = read_csv_file(path_file_csv)

    print("\nSaving JSON...")
    transform_json(df, path_file_json)

    print("\nSaving Parquet...")
    transform_parquet(df, path_file_parquet)

    print("\nReading JSON...")
    df_json = read_json_file(path_file_json)

    print("\nReading Parquet...")
    df_parquet = read_parquet_file(path_file_parquet)

    print("\nShape do CSV:")
    print(df.shape)

    print("\nShape do JSON:")
    print(df_json.shape)

    print("\nShape do Parquet")
    print(df_parquet.shape)


    print("\nExplorando dados...")
    explore_data(df)

    print("\nFiles generated successfully.")

#==EXECUÇÃO==#
if __name__ == "__main__":
    main()