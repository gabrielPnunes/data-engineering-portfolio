# Day 03 - API INGESTION

## Objetivo

- Inspecionar e entender estruturas JSON de APIs
- Aplicar diferentes estratégias de parsing
- Preparar dados semi-estruturados para análise tabular

---

## Funções criadas

- `inspect_json` - Inspeciona a estrutura do JSON de forma recursiva
  - parâmetros: `(data, level=0, max_level=3)`
  - exemplo: entender níveis e tipos de dados antes do parsing

- `parse_flat` - Converte JSON plano (lista de dicts) em DataFrame
  - parâmetros: `(data: list)`
  - exemplo: API com estrutura simples → transformação direta

- `parse_nested` - Extrai dados de um campo específico em JSON aninhado
  - parâmetros: `(data: dict, record_path: str)`
  - exemplo: acessar lista dentro de uma chave → `data['results']`

- `parse_normalize` - Normaliza JSON aninhado em formato tabular
  - parâmetros: `(data, record_path=None, meta=None)`
  - exemplo: transformar estruturas complexas em colunas

- `parse_select_fields` - Seleciona apenas campos específicos do JSON
  - parâmetros: `(data: list, fields: list)`
  - exemplo: reduzir dataset → apenas `data` e `valor`
