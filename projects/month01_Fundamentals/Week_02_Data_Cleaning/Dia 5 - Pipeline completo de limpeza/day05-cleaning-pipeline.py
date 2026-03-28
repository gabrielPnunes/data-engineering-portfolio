#==BIBLIOTECAS==#
import pandas as pd
from pathlib import Path
from src.extractor import extract
from src.cleaner import clean
from src.loader import load

#==CAMINHOS==#
BASE_DIR = Path("C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals")

input_path  = BASE_DIR / "Data/Raw/sales.csv"
output_path = BASE_DIR / "Data/Processed/sales_clean.parquet"

#==CONFIGS==#
SUPERSTORE_CONFIG = {
    "critical_columns": ["sale_id", "unit_price", "quantity", "total_price"],
    "string_columns":   ["branch", "city", "customer_type", "gender", "product_name", "product_category"],
    "numeric_columns":  ["unit_price", "quantity", "tax", "total_price", "reward_points"],
    "outlier_columns":  ["unit_price", "total_price"],
}

#==PIPELINE==#
def run_pipeline(input_path: Path, output_path: Path, config: dict) -> pd.DataFrame:
    print("=" * 40)
    print("[pipeline] Iniciando...")

    df = extract(input_path)
    df = clean(df, config=config)
    load(df, output_path)

    print("[pipeline] Concluído.")
    print("=" * 40)

    return df

#==EXECUTA==#
if __name__ == "__main__":
    result = run_pipeline(
        input_path=input_path,      
        output_path=output_path, 
        config=SUPERSTORE_CONFIG,
    )
    print(result.head())