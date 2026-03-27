#==BIBLIOTECAS==#
import os
import json
import pandas as pd
from pathlib import Path


#==FUNÇÕES PARA EXTRAIR DADOS==#
def extract(filepath: str | Path) -> pd.DataFrame:

    filepath = Path(filepath)
    extension = filepath.suffix.lstrip(".").lower()

    def read_json():
        with open(filepath, "r", encoding="utf-8") as f:
            conteudo = f.read().strip()

        if conteudo.startswith("{") and "\n" in conteudo:
            linhas = [json.loads(linha) for linha in conteudo.splitlines() if linha.strip()]
            return pd.DataFrame(linhas)

        dados = json.loads(conteudo)

        if isinstance(dados, list):
            return pd.DataFrame(dados)

        if isinstance(dados, dict):
            for chave, valor in dados.items():
                if isinstance(valor, list):
                    return pd.DataFrame(valor)
            return pd.DataFrame([dados])

    readers = {
        "csv":     lambda: pd.read_csv(filepath, encoding="latin-1"),
        "json":    read_json, 
        "parquet": lambda: pd.read_parquet(filepath),
        "xlsx":    lambda: pd.read_excel(filepath),
        "xls":     lambda: pd.read_excel(filepath),
    }

    if extension not in readers:
        raise ValueError(f"[extract] Formato não suportado: '.{extension}'")

    df = readers[extension]() 
    print(f"[extract] {len(df)} linhas extraídas de '{filepath}'")
    return df