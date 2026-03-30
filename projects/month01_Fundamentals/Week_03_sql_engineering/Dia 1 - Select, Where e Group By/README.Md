# Day 01 — SELECT, WHERE, GROUP BY
---
## O que foi feito
- Foi feito algumas queries para a exploração base de um dataset
- Utilizei e utilizarei o DuckDB por ser parecido com ferramentas que irei usar no futuro em questão de sintaxe
---
## Queries e funções criadas
---
- `get_connection` — lê um caminho de arquivo e registra a view no DuckDB
  - parâmetros: `(filepath: str, table_name: str)`

- `register_table` — registra e seleciona a tabela completa em uma conexão existente
  - parâmetros: `(con, filepath: str, table_name: str)`

- `select_columns` — seleciona colunas específicas de uma tabela
  - parâmetros: `(con, table: str, columns: list)` ex: `["col1", "col2"]`

- `filter_by_column` — filtra por uma coluna usando `IN`, aceita múltiplos valores
  - internamente usa um `for` que transforma a lista em `'A', 'B', 'C'` para o `WHERE ... IN (...)`
  - parâmetros: `(con, table: str, column: str, values: list)` ex: `["A", "B"]`

- `filter_by_value` — filtra por uma coluna usando operadores de comparação
  - operadores válidos: `>`, `<`, `>=`, `<=`, `=`, `!=`
  - parâmetros: `(con, table: str, column: str, value: float, operator: str)` ex: `(con, "orders", "total_price", 100, ">")`

- `group_by_column` — agrupa por uma coluna e retorna contagem, soma e média
  - parâmetros: `(con, table: str, group_col: str, agg_col: str)`

- `group_by_multiple_columns` — agrupa por múltiplas colunas usando um `for` para montar o `GROUP BY`
  - internamente transforma a lista em `"col1, col2"` para o SQL
  - parâmetros: `(con, table: str, group_cols: list, agg_col: str)` ex: `["branch", "product_category"]`

- `top_n_by_column` — retorna os top N registros de uma coluna por valor agregado
  - parâmetros: `(con, table: str, group_col: str, agg_col: str, top_n: int)` — `top_n` padrão é `10`