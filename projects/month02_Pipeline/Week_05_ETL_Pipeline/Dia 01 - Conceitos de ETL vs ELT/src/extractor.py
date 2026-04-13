# == BIBLIOTECAS == #
import pandas as pd
from pathlib import Path


# == FORMATOS SUPORTADOS == #
FORMATOS = {
    ".csv": pd.read_csv,
    ".parquet": pd.read_parquet,
    ".json": pd.read_json,
    ".xlsx": pd.read_excel,
    ".xls": pd.read_excel,
}


# == EXTRACT ARQUIVOS == #
def extract_file(path, **kwargs):

    path = Path(path)
    sufixo = path.suffix.lower()

    if sufixo not in FORMATOS:
        raise ValueError(f"Formato não suportado: {sufixo}")

    try:
        df = FORMATOS[sufixo](str(path), **kwargs)

        print(
            f"[extract] {path.name} | "
            f"{df.shape[0]} linhas | {df.shape[1]} colunas"
        )

        return df

    except Exception as e:
        print(f"[error] Falha ao carregar {path}: {e}")
        raise


# == VALIDAR SCHEMA == #
def validate_schema(df, expected_cols, name=None):

    faltante = set(expected_cols) - set(df.columns)

    if faltante:
        raise ValueError(
            f"[schema error] {name or ''} colunas faltando: {faltante}"
        )


# == EXTRAIR MULTIPLOS DATASETS == #
def extract_all_files(base_path, files):

    dfs = {}

    base_path = Path(base_path)

    for name, config in files.items():
        path = base_path / config.get("file")

        if not path.exists():
            print(f"[warning] Arquivo não encontrado: {path}")
            continue

        params = config.get("params", {})

        df = extract_file(path, **params)

        if "schema" in config:
            validate_schema(df, config["schema"], name)

        dfs[name] = df

    return dfs


# == INSPEÇÃO == #
def inspect(df, name=""):

    print(f"\n[inspect] {name}")

    print(f"\nShape: {df.shape}")

    memory_mb = round(df.memory_usage(deep=True).sum() / 1e6, 2)
    print(f"Memória: {memory_mb} MB")

    print("\nColunas:")
    print(list(df.columns))

    print("\nResumo estrutural:")
    summary = pd.DataFrame({
        "dtype": df.dtypes,
        "nulls": df.isna().sum(),
        "n_unique": df.nunique()
    })
    print(summary)

    print("\nEstatísticas numéricas:")
    print(df.describe(include="all"))

    print("\nAmostra:")
    print(df.head(5))