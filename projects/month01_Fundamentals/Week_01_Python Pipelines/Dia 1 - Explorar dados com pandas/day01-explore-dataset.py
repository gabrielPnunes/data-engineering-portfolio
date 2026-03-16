#==BLIBIOTECAS==#
import pandas as pd

path_file = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.csv"

#verificando o caminho
print(path_file)

#==FUNÇÃO PARA RETORNAR O CAMINHO EM CSV==#
def load_sales_dataset(path_file: str):
    df = pd.read_csv(path_file)
    return df   

#==FUNÇÃO PARA EXPLORAR OS DADOS==#
def explore_data(df: pd.DataFrame):
    print("\nPrimeiras Linhas")
    print(df.head())

    print("\nUltimas Linhas")
    print(df.tail())

    print("\nColunas")
    print(df.columns)

    print("\nInformações do DF")
    df.info()

    print("\nValores Nuloes")
    print(df.isnull().sum())

    print("\nEstrtura do DF (linhas X colunas)")
    print(df.shape)

    print("\nEstatisticas Basicas")
    print(df.describe())

df = load_sales_dataset(path_file)
explore_data(df)


#==TOP 10 produtos==#
top_dez_vendas = (
    df.groupby('sale_id')['total_price']
    .sum()
    .sort_values(ascending=False)
    .head(10)
)
print("\nTop 10 Vendas")
print(top_dez_vendas)

#==VENDAS TOTAL POR CATEGORIA==#
vendas_por_categoria = (
    df.groupby('product_category')['total_price']
    .sum()
    .sort_values(ascending = False)
    .head(10)
)
print("\nVendas Agrupadas por categoria")
print(vendas_por_categoria)

#==QUANTOS PEDIDOS FORAM REALIZADOS POR REGIÃO==#
print(df['city'].unique())
print(df['city'].value_counts())

pedidos_por_regiao = (
    df.groupby('city')['quantity']
    .sum()
    .sort_values()
)
print("\nTotal de pedidos por Região")
print(pedidos_por_regiao)