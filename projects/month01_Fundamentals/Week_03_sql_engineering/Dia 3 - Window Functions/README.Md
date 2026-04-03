# Day 03 - WINDOW FUNCTIONS

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


#==ROW NUMBERS==#
def row_number_by_partition(con: duckdb.DuckDBPyConnection, table: str, partition_col: str, order_col: str) -> pd.DataFrame:
    return con.execute(f"""
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {partition_col}
                ORDER BY {order_col} DESC
            ) AS row_num
        FROM {table}
    """).df()


#==RANKING==#
def rank_by_partition(con: duckdb.DuckDBPyConnection, table: str, partition_col: str, order_col: str) -> pd.DataFrame:
    return con.execute(f"""
        SELECT
            *,
            RANK() OVER (
                PARTITION BY {partition_col}
                ORDER BY {order_col} DESC
            ) AS rank
        FROM {table}
    """).df()


def dense_rank_by_partition(con: duckdb.DuckDBPyConnection, table: str, partition_col: str, order_col: str) -> pd.DataFrame:
    return con.execute(f"""
        SELECT
            *,
            DENSE_RANK() OVER (
                PARTITION BY {partition_col}
                ORDER BY {order_col} DESC
            ) AS dense_rank
        FROM {table}
    """).df()


#==LAG E LEAD==#
def lag_column(con: duckdb.DuckDBPyConnection, table: str, partition_col: str, order_col: str, value_col: str, offset: int = 1) -> pd.DataFrame:
    return con.execute(f"""
        SELECT
            *,
            LAG({value_col}, {offset}) OVER (
                PARTITION BY {partition_col}
                ORDER BY {order_col}
            ) AS prev_{value_col}
        FROM {table}
    """).df()


def lead_column(con: duckdb.DuckDBPyConnection, table: str, partition_col: str, order_col: str, value_col: str, offset: int = 1) -> pd.DataFrame:
    return con.execute(f"""
        SELECT
            *,
            LEAD({value_col}, {offset}) OVER (
                PARTITION BY {partition_col}
                ORDER BY {order_col}
            ) AS next_{value_col}
        FROM {table}
    """).df()


#==SOMAS E MEDIAS==#
def running_total(con: duckdb.DuckDBPyConnection, table: str, partition_col: str, order_col: str, value_col: str) -> pd.DataFrame:
    return con.execute(f"""
        SELECT
            *,
            ROUND(SUM({value_col}) OVER (
                PARTITION BY {partition_col}
                ORDER BY {order_col}
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 2) AS running_total
        FROM {table}
    """).df()


def moving_average(con: duckdb.DuckDBPyConnection, table: str, partition_col: str, order_col: str, value_col: str, window: int = 3) -> pd.DataFrame:
    return con.execute(f"""
        SELECT
            *,
            ROUND(AVG({value_col}) OVER (
                PARTITION BY {partition_col}
                ORDER BY {order_col}
                ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW
            ), 2) AS moving_avg_{window}
        FROM {table}
    """).df()