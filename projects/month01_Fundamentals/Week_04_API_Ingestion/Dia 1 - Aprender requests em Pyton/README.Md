# Day 01 - API INGESTION

## Objetivo
- Realizar ingestão de dados via API
- Tratar erros comuns em requisições HTTP
- Converter respostas JSON em DataFrame para análise

## Funções criadas

- `get` - Realiza requisição GET simples
  - parâmetros: `(url, params=None, headers=None)`
  - exemplo: consumir endpoint de API → obter dados JSON

- `safe_get` - Requisição GET com tratamento de erros
  - parâmetros: `(url, params=None, headers=None)`
  - exemplo: evitar quebra do pipeline em caso de falha na API

---

- `response_to_dataframe` - Converte resposta JSON para DataFrame
  - parâmetros: `(data, record_path=None)`
  - exemplo: transformar JSON em tabela → pronto para análise
