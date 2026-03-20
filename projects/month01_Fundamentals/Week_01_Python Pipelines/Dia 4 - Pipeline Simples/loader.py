#==BLIBIOTECA==#
import pandas as pd
import os
from pathlib import Path

#==CAMINHO GLOBAL e DE CONFIGURAÇÕES DE ARQUIVOS==#
output_path = Path("C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Staging/")
nome_arquivo = "test_pipeline.parquet"

#==FUNÇÃO LOADER==#
def load(df: pd.DataFrame, output_path: Path) -> None:
    os.makedirs(output_path, exist_ok = True)
    path_parquet = output_path / nome_arquivo
    df.to_parquet(path_parquet, index = False)
    print(f"[loader] {len(df)} linhas salvas em '{output_path}'")
    return 


# Criando um df de teste para validar o loader
df_teste = pd.DataFrame({
    "cidade": ["New York", "Chicago"],
    "vendas": [100, 200]
})