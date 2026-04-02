# Day 02 — JOINs

---

## O que foi feito
- Como tenho apenas uma tabela que é a `sales.csv`, foi criado views originadas dela para executar os joins
  
---

## Queries e funções criadas
---
- `join_tables` - unção que executa qualquer tipo de join
  - parâmetros: `(con, left: str, right: str, on: str | list, join_type: str, columns: list)`


- `inner_join` - Função que executa um inner_join
  - parâmetros: `(con, left: str, right: str, on: str | list, join_type: str, columns: list)`

- `left_join` - Função que executa um left_join
  - parâmetros: `(con, left: str, right:str, on: str | list, columns: list)`

- `right_join` - Função que executa um right_join
  - parâmetros: `(con, left: str, right:str, on: str | list, columns: list)`

- `full_join` - Função que executa um full join
  - parâmetros: `(con, left:str, right:str, on: str | list, coluumns: list)`

- `join_with_filter` - Função que executa qualquer join com filter
  - parâmetros: `(con, left:str, right:str, on: str | list, filter_col:str, filter_val:str, join_type:str, coluumns: list)`

---

## Views criadas

---

- `sales_member` — view agregada filtrando `customer_type = 'Member'`, agrupada por `product_name`, `product_category`, `city` e `branch`
  - colunas: `orders_member`, `revenue_member`, `avg_ticket_member`, `points_member`

- `sales_normal` — view agregada filtrando `customer_type = 'Normal'`, agrupada pelos mesmos campos
  - colunas: `orders_normal`, `revenue_normal`, `avg_ticket_normal`