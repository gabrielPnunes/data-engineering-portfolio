# Day 03 - API INGESTION (BCB DATA)

## Objetivo

- Consumir dados econômicos do Banco Central do Brasil (BCB)
- Estruturar séries temporais (SELIC, IPCA, câmbio)
- Padronizar dados para análise temporal

---

## Funções criadas

- `extract_serie` - Extrai dados de uma série pelo código
  - parâmetros: `(codigo, data_inicio, data_fim)`
  - exemplo: obter histórico da SELIC → série temporal

- `extract_by_name` - Extrai dados usando nome amigável da série
  - parâmetros: `(name, data_inicio, data_fim)`
  - exemplo: `extract_by_name('ipca', '01/01/2020', '01/01/2024')`

- `transform_serie` - Padroniza os dados extraídos
  - parâmetros: `(df, serie_name)`
  - exemplo: converter datas e valores → pronto para análise