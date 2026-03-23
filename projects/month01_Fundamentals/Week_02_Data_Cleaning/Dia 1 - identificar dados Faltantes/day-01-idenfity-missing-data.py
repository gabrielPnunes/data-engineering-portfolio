#==BIBLIOTECAS==#
import pandas as pd
import json

#==CAMINHOS==#
path_file = "C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.json"


#==FUNÇÕES==#
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


if __name__ == "__main__":
    df = extract(path_file)
    profile_summary(df)
    report = profile_missing(path_file)
    criticals = profile_critical(report)