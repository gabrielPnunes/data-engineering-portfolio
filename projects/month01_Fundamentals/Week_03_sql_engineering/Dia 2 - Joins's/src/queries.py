# ── BIBLIOTECA ────────────────────────────────────────────
import duckdb
import pandas as pd


# ── COLUNAS DISPONÍVEIS EM 'sales' ────────────────────────
# sale_id | branch | city | customer_type | gender |
# product_name | product_category | unit_price |
# quantity | tax | total_price | reward_points


# ── CONEXÃO ───────────────────────────────────────────────

def get_connection(filepath: str, table_name: str) -> duckdb.DuckDBPyConnection:
    """Cria uma conexão DuckDB em memória e registra o CSV como view."""
    con = duckdb.connect()
    con.execute(f"""
        CREATE OR REPLACE VIEW {table_name} AS
        SELECT * FROM read_csv_auto('{filepath}')
    """)
    print(f"[db] Tabela '{table_name}' registrada de '{filepath}'")
    return con


def register_table(
    con: duckdb.DuckDBPyConnection,
    filepath: str,
    table_name: str,
) -> None:
    """Registra uma tabela adicional na conexão existente."""
    con.execute(f"""
        CREATE OR REPLACE VIEW {table_name} AS
        SELECT * FROM read_csv_auto('{filepath}')
    """)
    print(f"[db] Tabela '{table_name}' registrada de '{filepath}'")


# ── SELECT ────────────────────────────────────────────────

def select_columns(
    con: duckdb.DuckDBPyConnection,
    table: str,
    columns: list,
) -> pd.DataFrame:
    """
    Retorna apenas as colunas solicitadas da tabela.

    Colunas disponíveis:
        sale_id, branch, city, customer_type, gender,
        product_name, product_category, unit_price,
        quantity, tax, total_price, reward_points
    """
    cols = ", ".join(columns)
    return con.execute(f"SELECT {cols} FROM {table}").df()


# ── WHERE (IN) ────────────────────────────────────────────

def filter_by_column(
    con: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    values: list,
) -> pd.DataFrame:
    """
    Filtra linhas onde `column` está em `values`.
    Usa parâmetros preparados para evitar SQL Injection.

    Exemplos:
        filter_by_column(con, "sales", "city",             ["New York", "Chicago"])
        filter_by_column(con, "sales", "customer_type",    ["Member"])
        filter_by_column(con, "sales", "product_name",     ["Apple", "Shampoo"])
        filter_by_column(con, "sales", "product_category", ["Beverages", "Fruits"])
        filter_by_column(con, "sales", "branch",           ["A"])
        filter_by_column(con, "sales", "gender",           ["Female"])
    """
    placeholders = ", ".join(f"${i + 1}" for i in range(len(values)))
    return con.execute(
        f"SELECT * FROM {table} WHERE {column} IN ({placeholders})",
        values,
    ).df()


# ── WHERE (operadores) ────────────────────────────────────

_OPERADORES_VALIDOS = [">", "<", ">=", "<=", "=", "!="]


def filter_by_value(
    con: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    value: float,
    operator: str = ">",
) -> pd.DataFrame:
    """
    Filtra linhas por operador de comparação em coluna numérica.

    Colunas numéricas: unit_price, quantity, tax, total_price, reward_points
    Operadores válidos: >, <, >=, <=, =, !=

    Exemplos:
        filter_by_value(con, "sales", "total_price",   100,  ">")
        filter_by_value(con, "sales", "reward_points", 0,    ">")
        filter_by_value(con, "sales", "quantity",      10,   ">=")
        filter_by_value(con, "sales", "unit_price",    5.00, "<=")
    """
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


# ── GROUP BY (uma coluna) ─────────────────────────────────

def group_by_column(
    con: duckdb.DuckDBPyConnection,
    table: str,
    group_col: str,
    agg_col: str,
) -> pd.DataFrame:
    """
    Agrupa por uma coluna e retorna total_orders, total_value e avg_value.

    Sugestões de group_col : branch, city, customer_type, gender,
                              product_name, product_category
    Sugestões de agg_col   : total_price, unit_price, quantity,
                              tax, reward_points
    """
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


# ── GROUP BY (múltiplas colunas) ──────────────────────────

def group_by_multiple_columns(
    con: duckdb.DuckDBPyConnection,
    table: str,
    group_cols: list,
    agg_col: str,
) -> pd.DataFrame:
    """
    Agrupa por várias colunas e retorna total_orders, total_value e avg_value.

    Exemplos de group_cols:
        ["branch", "customer_type"]
        ["city", "product_category"]
        ["gender", "product_name"]
        ["branch", "city", "customer_type"]
    """
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


# ── TOP N ─────────────────────────────────────────────────

def top_n_by_column(
    con: duckdb.DuckDBPyConnection,
    table: str,
    group_col: str,
    agg_col: str,
    top_n: int = 10,
) -> pd.DataFrame:
    """
    Retorna os top-N grupos ordenados por valor agregado decrescente.

    Exemplos:
        top_n_by_column(con, "sales", "product_name",     "total_price",   top_n=5)
        top_n_by_column(con, "sales", "city",             "total_price",   top_n=3)
        top_n_by_column(con, "sales", "product_category", "reward_points", top_n=6)
    """
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


# ── EXECUÇÃO ──────────────────────────────────────────────

filepath = r"C:\Users\Ceifas\OneDrive\Desktop\data-engineering-portfolio\projects\month01_Fundamentals\Data\Raw\sales.csv"

if __name__ == "__main__":
    con = get_connection(filepath, table_name="sales")

    print("\n── SELECT ──────────────────────────────────────────────────")
    print(select_columns(con, table="sales", columns=[
        "sale_id", "branch", "city", "product_name",
        "product_category", "total_price",
    ]))

    print("\n── WHERE city IN ['New York'] ───────────────────────────────")
    print(filter_by_column(con, table="sales", column="city", values=["New York"]))

    print("\n── WHERE total_price > 100 ─────────────────────────────────")
    print(filter_by_value(con, table="sales", column="total_price", value=100, operator=">"))

    print("\n── GROUP BY product_category ───────────────────────────────")
    print(group_by_column(con, table="sales", group_col="product_category", agg_col="total_price"))

    print("\n── GROUP BY branch + customer_type ─────────────────────────")
    print(group_by_multiple_columns(
        con, table="sales",
        group_cols=["branch", "customer_type"],
        agg_col="total_price",
    ))

    print("\n── TOP 5: product_name por total_price ─────────────────────")
    print(top_n_by_column(con, table="sales", group_col="product_name", agg_col="total_price", top_n=5))