#==BIBLIOTECA e FUNÇÕES==#
import duckdb
import pandas as pd
from queries import get_connection, register_table

#==CAMINHO==#
filepath = r"C:\Users\Ceifas\OneDrive\Desktop\data-engineering-portfolio\projects\month01_Fundamentals\Data\Raw\sales.csv"


_JOIN_TYPES_VALIDOS = ["INNER", "LEFT", "RIGHT", "FULL OUTER"]


def _build_on_clause(left: str, right: str, on: str | list) -> str:
    if isinstance(on, str):
        on = [on]
    return " AND ".join(f"{left}.{col} = {right}.{col}" for col in on)


def _validate_join_type(join_type: str) -> str:
    jt = join_type.upper()
    if jt not in _JOIN_TYPES_VALIDOS:
        raise ValueError(
            f"Join inválido '{join_type}'. Use um destes: {_JOIN_TYPES_VALIDOS}"
        )
    return jt


def join_tables(
    con: duckdb.DuckDBPyConnection,
    left: str,
    right: str,
    on: str | list,
    join_type: str = "INNER",
    columns: list = None,
) -> pd.DataFrame:

    jt = _validate_join_type(join_type)
    cols = ", ".join(columns) if columns else "*"
    on_clause = _build_on_clause(left, right, on)
    return con.execute(f"""
        SELECT {cols}
        FROM {left}
        {jt} JOIN {right}
            ON {on_clause}
    """).df()


#==INNER JOIN==#
def inner_join(
    con: duckdb.DuckDBPyConnection,
    left: str,
    right: str,
    on: str | list,
    columns: list = None,
) -> pd.DataFrame:
    return join_tables(con, left, right, on, join_type="INNER", columns=columns)

#==LEFT JOIN==#
def left_join(
    con: duckdb.DuckDBPyConnection,
    left: str,
    right: str,
    on: str | list,
    columns: list = None,
) -> pd.DataFrame:
    return join_tables(con, left, right, on, join_type="LEFT", columns=columns)

#==RIGHT JOIN==#
def right_join(
    con: duckdb.DuckDBPyConnection,
    left: str,
    right: str,
    on: str | list,
    columns: list = None,
) -> pd.DataFrame:
    return join_tables(con, left, right, on, join_type="RIGHT", columns=columns)


#==FULL JOIN==#
def full_outer_join(
    con: duckdb.DuckDBPyConnection,
    left: str,
    right: str,
    on: str | list,
    columns: list = None,
) -> pd.DataFrame:
    return join_tables(con, left, right, on, join_type="FULL OUTER", columns=columns)


#==JOINS COM FILTER==#
def join_with_filter(
    con: duckdb.DuckDBPyConnection,
    left: str,
    right: str,
    on: str | list,
    filter_col: str,
    filter_val: str,
    join_type: str = "LEFT",
    columns: list = None,
) -> pd.DataFrame:

    jt = _validate_join_type(join_type)
    cols = ", ".join(columns) if columns else "*"
    on_clause = _build_on_clause(left, right, on)
    return con.execute(f"""
        SELECT {cols}
        FROM {left}
        {jt} JOIN {right}
            ON {on_clause}
        WHERE {left}.{filter_col} = $1
    """, [filter_val]).df()