#==BIBLIOTECA==#
import duckdb
import pandas as pd

#==CAMINHO==#
filepath = r'C:\Users\AMD\Desktop\data-engineering-portfolio\projects\month01_Fundamentals\Data\Raw\sales.csv'

#==FUNÇÃO PARA CONEXÃO==#
def get_connection(filepath: str, table_name: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"""
        CREATE VIEW {table_name} AS
        SELECT 
            * 
        FROM 
            read_csv_auto('{filepath}')
    """)
    print(f"[db] Tabela '{table_name}' registrada de '{filepath}'")
    return con

def register_table(con: duckdb.DuckDBPyConnection, filepath: str, table_name: str) -> None:
    con.execute(f"""
        CREATE VIEW {table_name} AS
        SELECT * FROM read_csv_auto('{filepath}')
    """)
    print(f"[db] Tabela '{table_name}' registrada de '{filepath}'")


#==FUNÇÃO PARA O SELECT==#
def select_columns(con: duckdb.DuckDBPyConnection, table: str, columns: list) -> pd.DataFrame:
    cols = ", ".join(columns)
    return con.execute(f"""
        SELECT {cols}
        FROM {table}
    """).df()


#==FUNÇÃO O WHERE==#
def filter_by_column(con: duckdb.DuckDBPyConnection, table: str, column: str, values: list) -> pd.DataFrame:
    formatted = ", ".join(f"'{v}'" for v in values)
    return con.execute(f"""
        SELECT *
        FROM {table}
        WHERE {column} IN ({formatted})
    """).df()


#==FILTRAR POR OPERADORES==#
def filter_by_value(con: duckdb.DuckDBPyConnection, table: str, column: str, value: float, operator: str = ">") -> pd.DataFrame:
    operadores_validos = [">", "<", ">=", "<=", "=", "!="]
    if operator not in operadores_validos:
        raise ValueError(f"Operador inválido! Use um destes: {operadores_validos}")
    return con.execute(f"""
        SELECT *
        FROM {table}
        WHERE {column} {operator} {value}
        ORDER BY {column} DESC
    """).df()


#==AGRUPAR POR UMA COLUNA==#
def group_by_column(con: duckdb.DuckDBPyConnection, table: str, group_col: str, agg_col: str) -> pd.DataFrame:
    return con.execute(f"""
        SELECT
            {group_col},
            COUNT(*)                    AS total_orders,
            ROUND(SUM({agg_col}), 2)    AS total_value,
            ROUND(AVG({agg_col}), 2)    AS avg_value
        FROM {table}
        GROUP BY {group_col}
        ORDER BY total_value DESC
    """).df()


#==AGRUPAR POR MÚLTIPLAS COLUNAS==#
def group_by_multiple_columns(con: duckdb.DuckDBPyConnection, table: str, group_cols: list, agg_col: str) -> pd.DataFrame:
    cols = ", ".join(group_cols)
    return con.execute(f"""
        SELECT
            {cols},
            COUNT(*)                    AS total_orders,
            ROUND(SUM({agg_col}), 2)    AS total_value,
            ROUND(AVG({agg_col}), 2)    AS avg_value
        FROM {table}
        GROUP BY {cols}
        ORDER BY total_value DESC
    """).df()


#==TOP N POR COLUNA==#
def top_n_by_column(con: duckdb.DuckDBPyConnection, table: str, group_col: str, agg_col: str, top_n: int = 10) -> pd.DataFrame:
        return con.execute(f"""
        SELECT
            {group_col},
            COUNT(*)                    AS total_orders,
            ROUND(SUM({agg_col}), 2)    AS total_value
        FROM {table}
        GROUP BY {group_col}
        ORDER BY total_value DESC
        LIMIT {top_n}
    """).df()

"""
#==TESTS==#
if __name__ == "__main__":
    con = get_connection(filepath, table_name="sales")

    print("\n #==SELECT==#")
    print(select_columns(con, table="sales", columns=[
        "sale_id", "branch", "city", "product_name", "product_category", "total_price"
    ]))

    city_filter = "New York"
    print(f"\n #==WHERE: city = {city_filter}==#")
    print(filter_by_column(con, table="sales", column="city", values=[city_filter]))

    price_value = 100
    price_operator = ">"
    print(f"\n #==WHERE: total_price {price_operator} {price_value}==#")
    print(filter_by_value(con, table="sales", column="total_price", value=price_value, operator=price_operator))

    print("\n #==GROUP BY: product_category==#")
    print(group_by_column(con, table="sales", group_col="product_category", agg_col="total_price"))

    print("\n #==GROUP BY: branch + customer_type==#")
    print(group_by_multiple_columns(con, table="sales", group_cols=["branch", "customer_type"], agg_col="total_price"))

    top_n = 2
    print(f"\n #==TOP {top_n}: product_name==#")
    print(top_n_by_column(con, table="sales", group_col="product_name", agg_col="total_price", top_n=top_n))
    """