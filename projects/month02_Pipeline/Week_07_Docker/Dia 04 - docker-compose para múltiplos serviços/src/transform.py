#blibioteca
import pandas as pd


#==mudar tipos==#
def cast_types(df, type_map):
    df = df.copy()

    for col, dtype in type_map.items():
        if col not in df.columns:
            print(f"[transform] Coluna '{col}' não encontrada - pulando")
            continue

        try:
            if dtype == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")

            elif dtype == "float":
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(",", ".", regex=False)
                    .str.replace(r"[^\d\.]", "", regex=True)
                    .replace("", None)
                    .astype(float)
                )

            elif dtype == "int":
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(r"[^\d]", "", regex=True)
                    .replace("", None)
                    .astype(float)
                    .astype("Int64")
                )

            else:
                df[col] = df[col].astype(dtype, errors="ignore")

            print(f"[transform] '{col}' -> {dtype}")

        except Exception as e:
            print(f"[error] Falha ao converter '{col}' para {dtype}: {e}")

    return df


#==dropar nulos==#
def drop_nulls(df, subset, name=""):
    before = len(df)
    df = df.dropna(subset=subset).reset_index(drop=True)
    after = len(df)

    print(f"[transform] '{name}' - dropna {subset}: {before - after} linhas removidas")
    return df


#==preencher nulos==#
def fill_nulls(df, fill_map):
    df = df.copy()

    for col, value in fill_map.items():
        if col in df.columns:
            nulls = df[col].isna().sum()
            df[col] = df[col].fillna(value)
            print(f"[transform] '{col}' - {nulls} nulos preenchidos com '{value}'")
        else:
            print(f"[transform] Coluna '{col}' não encontrada - pulando")

    return df


#==adicionar colunas==#
def add_columns(df, columns):
    df = df.copy()

    for col, func in columns.items():
        try:
            df[col] = func(df)
            print(f"[transform] Coluna '{col}' criada")
        except Exception as e:
            print(f"[error] Falha ao criar '{col}': {e}")

    return df


#==filter==#
def filter_rows(df, filters, name=""):
    before = len(df)
    df = df.copy()

    for col, values in filters.items():
        if col not in df.columns:
            print(f"[transform] Coluna '{col}' não existe - pulando")
            continue

        if isinstance(values, list):
            df = df[df[col].isin(values)]
        else:
            df = df[df[col] == values]

    after = len(df)

    print(f"[transform] '{name}' — filter {filters}: {before - after} linhas removidas")
    return df.reset_index(drop=True)


#==joins==#
def joins(left, right, on, how="left", cols=None):

    if on not in left.columns:
        raise ValueError(f"[enrich] Coluna '{on}' não existe no left")

    if on not in right.columns:
        raise ValueError(f"[enrich] Coluna '{on}' não existe no right")

    if right[on].duplicated().any():
        print(f"[warning] '{on}' possui duplicatas no dataset de join")

    if cols:
        cols = [c for c in cols if c != on]
        right = right[[on] + cols]

    df = left.merge(right, on=on, how=how)

    print(f"[transform] enrich on '{on}' ({how}) - shape: {df.shape}")
    return df