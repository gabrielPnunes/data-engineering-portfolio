#==BLIBIOTECAS E FUNÇÕES==#
import pandas as pd
from extractor import extract
from cleaner import clean
from loader import load

#==FUNÇÕES DO PIPELINE==#
def run_pipeline(input_path: str, output_path: str, config: dict) -> pd.DataFrame:
    """Pipeline completo: extract → clean → load."""
    print("=" * 40)
    print("[pipeline] Iniciando...")

    df = extract(input_path)
    df = clean(df, config=config)
    load(df, output_path)

    print("[pipeline] Concluído.")
    print("=" * 40)

    return df