# Day 04 - DATA MODELING (GENERIC & ESCALÁVEL)

## Objetivo
- Generalizar a criação de dimensões e fato
- Tornar o pipeline reutilizável e escalável

---

## Estrutura do modelo
- Tabela fato: `fact_sales`
- Tabelas dimensão criadas dinamicamente:
  - `dim_customer`
  - `dim_product`
  - `dim_location`

---

## Queries e funções criadas

- `setup_duckdb` - Configura parâmetros de performance do DuckDB
  - parâmetros: `(con, memory_limit='1GB', threads=2)`
  
---

- `create_dimension` - Cria dimensões de forma genérica a partir de qualquer tabela
  - parâmetros: `(con, source_table, dim_table, columns, id_column, order_by=None)`
  - exemplo: criação dinâmica de dimensões → evita repetição de código

---

- `create_fact` - Cria a tabela fato dinamicamente com base nas dimensões informadas
  - parâmetros: `(con, source_table, fact_table, fact_columns, dimensions)`
  - exemplo: construção automatizada do modelo estrela (star schema)

---

## Queries analíticas

- `aggregate` - Realiza agregações dinâmicas com base em dimensões
  - parâmetros: `(con, fact_table, dim_table, join_on, group_columns, metric_column, agg='SUM', limit=10)`
  - exemplo: receita por categoria → `SUM(total_price)`
