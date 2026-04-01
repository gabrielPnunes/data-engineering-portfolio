#==BLIBIOTECAS E FUNÇÕES==#
import duckdb
import pandas as pd
from queries import get_connection


#==ROW NUMBERS==#
def row_number_by_partition(con: duckdb.DuckDBPyConnection, table: str, partition_col: str, order_col: str) -> pd.DataFrame:
    """Numera linhas dentro de cada partição ordenadas por coluna."""
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
    """Rankeia linhas dentro de cada partição — empates recebem o mesmo rank."""
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
    """DENSE_RANK — igual ao RANK mas sem pular números após empates."""
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
    """LAG — traz o valor da linha anterior dentro da partição."""
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
    """LEAD — traz o valor da próxima linha dentro da partição."""
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
    """Calcula acumulado (running total) dentro de cada partição."""
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
    """Calcula média móvel dentro de cada partição."""
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


#==EXECUÇÃO==#
if __name__ == "__main__":
    con = get_connection("data/sales.csv", table_name="sales")

    print("\n── ROW_NUMBER por category ─────────")
    print(row_number_by_partition(con,
        table="sales",
        partition_col="product_category",
        order_col="total_price"
    ).head(10))

    print("\n── RANK por city ───────────────────")
    print(rank_by_partition(con,
        table="sales",
        partition_col="city",
        order_col="total_price"
    ).head(10))

    print("\n── DENSE_RANK por branch ───────────")
    print(dense_rank_by_partition(con,
        table="sales",
        partition_col="branch",
        order_col="total_price"
    ).head(10))

    print("\n── LAG: venda anterior por branch ──")
    print(lag_column(con,
        table="sales",
        partition_col="branch",
        order_col="sale_id",
        value_col="total_price"
    ).head(10))

    print("\n── LEAD: próxima venda por branch ──")
    print(lead_column(con,
        table="sales",
        partition_col="branch",
        order_col="sale_id",
        value_col="total_price"
    ).head(10))

    print("\n── RUNNING TOTAL por category ──────")
    print(running_total(con,
        table="sales",
        partition_col="product_category",
        order_col="sale_id",
        value_col="total_price"
    ).head(10))

    print("\n── MOVING AVERAGE por branch ───────")
    print(moving_average(con,
        table="sales",
        partition_col="branch",
        order_col="sale_id",
        value_col="total_price",
        window=3
    ).head(10))