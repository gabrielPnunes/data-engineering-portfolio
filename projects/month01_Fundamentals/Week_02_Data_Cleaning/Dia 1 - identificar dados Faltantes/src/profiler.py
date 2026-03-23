import pandas as pd
from extractor import extract

def profile_missing(file_path: str) -> pd.DataFrame:
    df = extract(file_path)
    total = len(df)

    report = pd.DataFrame({
        "coluna":     df.columns,
        "nulos":      df.isnull().sum().values,
        "percentual": (df.isnull().sum().values / total * 100).round(2),
        "tipos":      df.dtypes.values,
    })

    return report


def profile_summary(df: pd.DataFrame):
    print("\n[summary] Shape: ", df.shape)
    print("\n[summary] Tipos de dados: ")
    print(df.dtypes.to_string())
    print("\n[summary] Duplicados: ", df.duplicated().sum())

def profile_critical(report: pd.DataFrame, limite: float = 10) -> pd.DataFrame:
    criticals = (
        report[report["percentual"] >= limite]
        .sort_values("percentual", ascending=False)
        .reset_index(drop=True)
    )
    print(f"\n[critical] {len(criticals)} colunas com >= {limite}% de nulos:")
    print(criticals.to_string(index=False))
    return criticals