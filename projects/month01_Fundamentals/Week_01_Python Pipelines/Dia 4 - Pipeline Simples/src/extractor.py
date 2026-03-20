#==BIBLIOTECAS==# 
import pandas as pd
from pathlib import Path

#==CAMINHO GLOBAL==#
path_file = Path("C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.csv")

#==FUNÇÃO DE EXTRAÇÃO==#
def extract(path_file) -> pd.DataFrame:
    extensions = str(path_file).split(".")[-1].lower()
    reader = {
        "csv":      lambda: pd.read_csv(path_file, encoding="latin-1", sep=","),
        "xlsx":     lambda: pd.read_excel(path_file),
        "xls":      lambda: pd.read_excel(path_file),
        "json":     lambda: pd.read_json(path_file),
        "parquet":  lambda: pd.read_parquet(path_file)
    }
    if extensions not in reader:
        raise ValueError(f"[extractor] Formato não suportado: '{extensions}'")

    df = reader[extensions]()
    print(f"\n[extractor] {len(df)} linhas extraídas de: '{path_file}'") 
    return df