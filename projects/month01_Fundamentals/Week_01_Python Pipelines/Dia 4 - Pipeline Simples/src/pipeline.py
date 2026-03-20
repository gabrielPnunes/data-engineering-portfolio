#==BLIBIOTECAS E FUNÇÕES==#
#funções
from extractor import extract
from transformer import (
    transform_normalize_columns,
    transform_filtred_by_column_value,
    transform_select_columns,
)
from loader import load
#blibiotecas
from pathlib import Path

#==FUNÇÕES==#
def run_pipeline(input_path: str, column: str, value: str, output_path: str):
    """Orquestra o pipeline: extract → transform → load."""

    print("=" * 40)
    print("[pipeline] Iniciando...")

    df = extract(input_path)

    df = transform_normalize_columns(df)
    df = transform_filtred_by_column_value(df, column, value)
    df = transform_select_columns(df, columns=[
        "order_id", "order_date", "region",
        "category", "sales", "profit"
    ])

    load(df, output_path)

    print("[pipeline] Concluído.")
    print("=" * 40)

    return df


#==EXECUÇÃO==#
if __name__ == "__main__":
    result = run_pipeline(
        input_path="data/orders.csv",
        region="West",
        output_path="data/output/west_orders.parquet",
    )
    print(result.head())