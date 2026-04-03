#==BLIBIOTECAS==#
import duckdb
import pandas as pd

#==FUNÇÃO PARA OBTER CONEXÃO==#
def get_connection(filepath: str, table_name: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"""
        CREATE OR REPLACE VIEW {table_name} AS
        SELECT * FROM read_csv_auto('{filepath}')
    """)
    print(f"[db] Tabela '{table_name}' registrada de '{filepath}'")
    return con

#+=FUNÇÃO PARA REGISTER TABELA==#
def register_table(
    con: duckdb.DuckDBPyConnection,
    filepath: str,
    table_name: str,
) -> None:
    con.execute(f"""
        CREATE OR REPLACE VIEW {table_name} AS
        SELECT * FROM read_csv_auto('{filepath}')
    """)
    print(f"[db] Tabela '{table_name}' registrada de '{filepath}'")


#==FUNÇÃO PARA SELECIONAR TABELA ESPECIFICA==#
def select_columns(
    con: duckdb.DuckDBPyConnection,
    table: str,
    columns: list,
) -> pd.DataFrame:
    cols = ", ".join(columns)
    return con.execute(f"SELECT {cols} FROM {table}").df()


#==FUNÇÃO PARA FILTER POR COLUNA==#
def filter_by_column(
    con: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    values: list,
) -> pd.DataFrame:
    placeholders = ", ".join(f"${i + 1}" for i in range(len(values)))
    return con.execute(
        f"SELECT * FROM {table} WHERE {column} IN ({placeholders})",
        values,
    ).df()


#==OPERADORES==#
_OPERADORES_VALIDOS = [">", "<", ">=", "<=", "=", "!="]


#==FILTRAR COMO VALOR, É UM WHERE==#
def filter_by_value(
    con: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    value: float,
    operator: str = ">",
) -> pd.DataFrame:
    
    if operator not in _OPERADORES_VALIDOS:
        raise ValueError(
            f"Operador inválido '{operator}'. Use um destes: {_OPERADORES_VALIDOS}"
        )
    return con.execute(f"""
        SELECT *
        FROM {table}
        WHERE {column} {operator} {value}
        ORDER BY {column} DESC
    """).df()


#==FUNÇÃO PARA AGREGAMENTO EM APENAS UMA COLUNA==#
def group_by_column(
    con: duckdb.DuckDBPyConnection,
    table: str,
    group_col: str,
    agg_col: str,
) -> pd.DataFrame:
    return con.execute(f"""
        SELECT
            {group_col},
            COUNT(*)                 AS total_orders,
            ROUND(SUM({agg_col}), 2) AS total_value,
            ROUND(AVG({agg_col}), 2) AS avg_value
        FROM {table}
        GROUP BY {group_col}
        ORDER BY total_value DESC
    """).df()


#==FAZER UM AGRUPAPAMENTO POR VARIAS COLUNAS==#
def group_by_multiple_columns(
    con: duckdb.DuckDBPyConnection,
    table: str,
    group_cols: list,
    agg_col: str,
) -> pd.DataFrame:
    cols = ", ".join(group_cols)
    return con.execute(f"""
        SELECT
            {cols},
            COUNT(*)                 AS total_orders,
            ROUND(SUM({agg_col}), 2) AS total_value,
            ROUND(AVG({agg_col}), 2) AS avg_value
        FROM {table}
        GROUP BY {cols}
        ORDER BY total_value DESC
    """).df()


#==USADA PARA FAZER RANKINGS==#
def top_n_by_column(
    con: duckdb.DuckDBPyConnection,
    table: str,
    group_col: str,
    agg_col: str,
    top_n: int = 10,
) -> pd.DataFrame:
    return con.execute(f"""
        SELECT
            {group_col},
            COUNT(*)                 AS total_orders,
            ROUND(SUM({agg_col}), 2) AS total_value
        FROM {table}
        GROUP BY {group_col}
        ORDER BY total_value DESC
        LIMIT {top_n}
    """).df()


#TESTES#
filepath = r"C:\Users\Ceifas\OneDrive\Desktop\data-engineering-portfolio\projects\month01_Fundamentals\Data\Raw\sales.csv"

if __name__ == "__main__":
    con = get_connection(filepath, table_name="sales")

    print("\n ====SELECT==== ")
    print(select_columns(con, table="sales", columns=[
        "sale_id", "branch", "city", "product_name",
        "product_category", "total_price",
    ]))

    print("\n ====WHERE city IN ['New York']==== ")
    print(filter_by_column(con, table="sales", column="city", values=["New York"]))

    print("\n ====WHERE total_price > 100==== ")
    print(filter_by_value(con, table="sales", column="total_price", value=100, operator=">"))

    print("\n ====GROUP BY product_category==== ")
    print(group_by_column(con, table="sales", group_col="product_category", agg_col="total_price"))

    print("\n ====GROUP BY branch + customer_type==== ")
    print(group_by_multiple_columns(
        con, table="sales",
        group_cols=["branch", "customer_type"],
        agg_col="total_price",
    ))

    print("\n ====TOP 5: product_name por total_price==== ")
    print(top_n_by_column(con, table="sales", group_col="product_name", agg_col="total_price", top_n=5))