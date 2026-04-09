# Day 04 - DATA STORAGE (UTILS DE PERSISTÊNCIA)

## Objetivo

- Criar utilitários para salvar e carregar dados
- Padronizar armazenamento em múltiplos formatos
- Facilitar reuso e validação de dados no pipeline

## Funções criadas

- `save_json` - Salva dados brutos em formato JSON
  - parâmetros: `(data, output_path: str)`
  - exemplo: persistir resposta original da API

- `save_csv` - Salva DataFrame em formato CSV
  - parâmetros: `(df: pd.DataFrame, output_path: str)`
  - exemplo: exportar dados para uso geral

- `save_parquet` - Salva DataFrame em formato Parquet
  - parâmetros: `(df: pd.DataFrame, output_path: str)`
  - exemplo: armazenamento otimizado para análise

- `save_all_formats` - Salva dados em JSON, CSV e Parquet simultaneamente
  - parâmetros: `(df: pd.DataFrame, data_raw, base_path: str, filename: str)`
  - exemplo: pipeline completo → raw + processed

- `load_json` - Carrega dados JSON
  - parâmetros: `(filepath: str)`
  - exemplo: validar dados brutos

- `load_csv` - Carrega dados CSV
  - parâmetros: `(filepath: str)`
  - exemplo: leitura simples para análise
  
- `load_parquet` - Carrega dados Parquet
  - parâmetros: `(filepath: str)`
  - exemplo: leitura performática para grandes volumes
