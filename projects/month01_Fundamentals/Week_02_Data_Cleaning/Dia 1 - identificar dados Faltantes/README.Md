# Day 01 — Identifying Missing Data
---
## O que foi feito
Análise de valores faltantes no dataset Superstore com geração
de relatório por coluna: total de nulos, percentual e dtype,
por enquanto vou usando as funções apenas copiando o arquivo no src
depois busco uma forma melhor de usar reutilizar as funções

---
## Funções criadas      
- `extract` — lê qualquer formato: CSV, JSON, Parquet, Excel
- `profile_summary` — exibe shape, tipos de dados e total de duplicatas
- `profile_missing` — gera relatório de nulos por coluna com percentual
- `profile_critical` — filtra colunas com mais de 10% de valores nulos