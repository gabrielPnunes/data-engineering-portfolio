#==BLIBIOTECAS E FUNÇÕES==#
import duckdb
import pandas as pd

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