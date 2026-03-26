#==BLIBIOTECAS==#
import pandas as pd
import os


#==FUNÇÕES PARA CARREGAMENTO==#
def load(df: pd.DataFrame, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok= True)
    df.to_parquet(output_path, index = False)
    print(f"[load] {len(df)} linhas salvas em '{output_path}'")