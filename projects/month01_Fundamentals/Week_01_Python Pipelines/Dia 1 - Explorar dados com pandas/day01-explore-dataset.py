#==BIBLIOTECAS==#
import pandas as pd
from pathlib import Path


#==CONFIGURAÇÃO DO CAMINHO==#
path_file = Path("C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.csv")

# Verificando o caminho
print(f"Caminho do arquivo: {path_file}")


#==LOAD: FUNÇÃO PARA CARREGAR O CSV==#
def load_data(path_file: Path) -> pd.DataFrame:
    if not path_file.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path_file}")
    df = pd.read_csv(path_file)
    print(f"Arquivo carregado com sucesso! ({df.shape[0]} linhas x {df.shape[1]} colunas)")
    return df


#==EXPLORE: FUNÇÃO PARA EXPLORAR OS DADOS==#
def explore_data(df: pd.DataFrame) -> None:
    print("\n--- Primeiras Linhas ---")
    print(df.head())

    print("\n--- Últimas Linhas ---")
    print(df.tail())

    print("\n--- Colunas ---")
    print(df.columns.tolist())

    print("\n--- Informações do DataFrame ---")
    df.info()

    print("\n--- Valores Nulos ---")
    print(df.isnull().sum())

    print("\n--- Estrutura do DataFrame (linhas x colunas) ---")
    print(df.shape)

    print("\n--- Estatísticas Básicas ---")
    print(df.describe())


#==TRANSFORM: FUNÇÃO COM AS ANÁLISES==#
def transform(df: pd.DataFrame) -> dict:

    # Top 10 produtos por receita total
    top_dez_produtos = (
        df.groupby('product_name')['total_price']
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    print("\n--- Top 10 Produtos por Receita ---")
    print(top_dez_produtos)

    # Vendas totais por categoria
    vendas_por_categoria = (
        df.groupby('product_category')['total_price']
        .sum()
        .sort_values(ascending=False)
    )
    print("\n--- Vendas Agrupadas por Categoria ---")
    print(vendas_por_categoria)

    # Número de pedidos por cidade/região
    print("\n--- Cidades únicas ---")
    print(df['city'].unique())

    pedidos_por_regiao = (
        df.groupby('city')['sale_id']
        .count()
        .sort_values(ascending=False)
        .rename('total_pedidos')
    )
    print("\n--- Total de Pedidos por Região ---")
    print(pedidos_por_regiao)


#==PIPELINE PRINCIPAL==#
if __name__ == "__main__":

    # 1. Extract
    df = load_data(path_file)

    # 2. Exploração
    explore_data(df)
