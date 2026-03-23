import pandas as pd
import json

def extract(file_path: str) -> pd.DataFrame:
    extension = file_path.split(".")[-1].lower()

    def read_json():                                       
        with open(file_path, "r", encoding="utf-8") as f:
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
            return pd.DataFrame(dados)

        raise ValueError(f"[extract] Estrutura JSON não reconhecida em: '{file_path}'")

    readers = {
        "csv":     lambda: pd.read_csv(file_path, encoding="latin-1", sep=","),
        "xlsx":    lambda: pd.read_excel(file_path),
        "xls":     lambda: pd.read_excel(file_path),
        "json":    lambda: read_json(),
        "parquet": lambda: pd.read_parquet(file_path)
    }

    if extension not in readers:
        raise ValueError(f"[extract] Formato não suportado: '.{extension}'")

    df = readers[extension]()
    print(f"[extract] {len(df)} linhas extraídas de: '{file_path}'")
    return df
