# Day 02 - EXTRACT (Ingestão + Inspeção + Validação Inicial)

---

## Objetivo

- Estruturar a camada de ingestão de dados (EXTRACT)  
- Consolidar carregamento de múltiplos arquivos  
- Adicionar inspeção inicial dos dados  
- Implementar validações básicas (nulos e duplicatas)  
- Gerar um resumo padronizado da extração  

---

## Funções criadas

- `load_all` - Carrega múltiplos datasets da camada RAW  
  - parâmetros: `(path: str)`  
  - exemplo: leitura de arquivos CSV em lote  

- `inspect` - Inspeciona estrutura dos dados  
  - parâmetros: `(df: pd.DataFrame, name: str)`  
  - exemplo: visualizar schema, tipos e volume  

- `extract_summary` - Gera resumo da extração  
  - parâmetros: `(dfs: dict)`  
  - exemplo: total de linhas, colunas e nulos por arquivo  

- `check_duplicates` - Valida duplicatas em coluna chave  
  - parâmetros: `(df: pd.DataFrame, key_col: str, name: str = "")`  
  - exemplo: identificar problemas de chave primária  

---

## Funcionalidades adicionadas

### Resumo da extração

Geração automática de um DataFrame consolidado com:

- Nome do arquivo  
- Quantidade de linhas  
- Quantidade de colunas  
- Total de valores nulos  