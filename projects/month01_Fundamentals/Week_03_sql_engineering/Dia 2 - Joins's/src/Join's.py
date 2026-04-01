#===BLIBIOTECA=#
import duckdb
import pandas as pd
from queries import get_connection, register_table

filepath = r'C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.csv'

#==FUNÇÕES PARA JOINS==#
def inner_join(con: duckdb.DuckDBPyConnection, left: str, right: str, on: str, columns: list = None) -> pd.DataFrame:
    cols = ", ".join(columns) if columns else "*"
    return con.execute(f"""
        SELECT {cols}
        FROM {left} 
        INNER JOIN {right}
            ON {left}.{on} = {right}.{on}
    """).df()


def left_join(con: duckdb.DuckDBPyConnection, left: str, right: str, on: str, columns: list = None) -> pd.DataFrame:
    cols = ", ".join(columns) if columns else "*"
    return con.execute(f"""
        SELECT {cols}
        FROM {left}
        LEFT JOIN {right}
            ON {left}.{on} = {right}.{on}
    """).df()


def right_join(con: duckdb.DuckDBPyConnection, left: str, right: str, on: str, columns: list = None) -> pd.DataFrame:
    cols = ", ".join(columns) if columns else "*"
    return con.execute(f"""
        SELECT {cols}
        FROM {left}
        RIGHT JOIN {right}
            ON {left}.{on} = {right}.{on}
    """).df()


def full_outer_join(con: duckdb.DuckDBPyConnection, left: str, right: str, on: str, columns: list = None) -> pd.DataFrame:
    cols = ", ".join(columns) if columns else "*"
    return con.execute(f"""
        SELECT {cols}
        FROM {left}
        FULL OUTER JOIN {right}
            ON {left}.{on} = {right}.{on}
    """).df()


def join_with_filter(con: duckdb.DuckDBPyConnection, left: str, right: str, on: str, filter_col: str, filter_val: str) -> pd.DataFrame:
    return con.execute(f"""
        SELECT *
        FROM {left}
        LEFT JOIN {right}
            ON {left}.{on} = {right}.{on}
        WHERE {left}.{filter_col} = '{filter_val}'
    """).df()


#==TEST
if __name__ == "__main__":
    con = get_connection(filepath, table_name="sales")
    register_table(con, filepath, table_name="customers")

    print("\n ====INNER JOIN==== ")
    print(inner_join(con, left="sales", right="customers", on="customer_id", columns=[
        "sales.sale_id",
        "sales.product_name",
        "sales.total_price",
        "customers.customer_name",
        "customers.email",
    ]))

    print("\n ====SELECT JOIN==== ")
    print(left_join(con, left="sales", right="customers", on="customer_id", columns=[
        "sales.sale_id",
        "sales.product_name",
        "customers.customer_name",
    ]))

    print("\n ====RIGHT JOIN==== ")
    print(right_join(con, left="sales", right="customers", on="customer_id", columns=[
        "sales.sale_id",
        "customers.customer_name",
        "customers.email",
    ]))

    print("\n ====FULL OUTER JOIN==== ")
    print(full_outer_join(con, left="sales", right="customers", on="customer_id"))

    print("\n ====LEFT JOIN + FILTER: New York==== ")
    print(join_with_filter(con, left="sales", right="customers", on="customer_id",
        filter_col="city", filter_val="New York"
    ))
    