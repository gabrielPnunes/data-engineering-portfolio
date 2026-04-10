# Day 05 - DATA PIPELINE (INGESTION + VALIDAÇÃO + REPORT)

---

## Objetivo

- Construir um pipeline completo de ingestão de dados
- Adicionar validação e controle de qualidade
- Gerar relatórios básicos para análise exploratória

---

## Queries e funções criadas

- `extract_serie` - Extrai e transforma uma série econômica
  - parâmetros: `(serie_name: str, date_start: str, date_end: str)`
  - exemplo: carregar dados da API já tratados

- `validate` - Valida integridade do DataFrame
  - parâmetros: `(df: pd.DataFrame, serie_name: str)`
  - exemplo: garantir que dados não estão vazios ou inválidos

- `report` - Gera estatísticas básicas da série
  - parâmetros: `(df: pd.DataFrame, serie_name: str)`
  - exemplo: mínimo, máximo, média e período

- `run_pipeline` - Executa o pipeline completo
  - parâmetros: `(config: dict, output_path: str)`
  - exemplo: processar múltiplas séries automaticamente
