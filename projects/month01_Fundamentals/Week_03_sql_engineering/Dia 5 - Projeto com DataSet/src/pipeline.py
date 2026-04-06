# src/pipeline.py

import duckdb
import pandas as pd


def top_by_partition(
    con: duckdb.DuckDBPyConnection,
    fact_table: str,
    dim_partition: str,
    dim_metric: str,
    partition_col: str,
    metric_col: str,
    group_cols: list,
    rank: int = 1
) -> pd.DataFrame:
    """Top N registros por partição usando QUALIFY + RANK.
    
    dim_partition — tabela dimensão usada para particionar (ex: dim_location)
    dim_metric    — tabela dimensão usada para a métrica (ex: dim_product)
    partition_col — coluna de join da partição (ex: location_id)
    metric_col    — coluna de join da métrica (ex: product_id)
    group_cols    — colunas para agrupar e exibir (ex: ['city', 'product_name'])
    rank          — posição máxima do ranking a retornar
    """
    p = dim_partition.split('_', 1)[1][0]
    m = dim_metric.split('_', 1)[1][0]

    group    = ', '.join(f"{p}.{c}" if c in _get_columns(con, dim_partition) else f"{m}.{c}" for c in group_cols)
    order_by = f"SUM(f.{_infer_metric(con, fact_table)})"

    return con.execute(f"""
        SELECT
            {group},
            ROUND(SUM(f.{_infer_metric(con, fact_table)}), 2)  AS total_revenue,
            COUNT(*)                                            AS total_orders
        FROM {fact_table} f
        LEFT JOIN {dim_partition} {p} USING ({partition_col})
        LEFT JOIN {dim_metric}    {m} USING ({metric_col})
        GROUP BY {group}
        QUALIFY RANK() OVER (
            PARTITION BY {p}.{_get_columns(con, dim_partition)[1]}
            ORDER BY {order_by} DESC
        ) <= {rank}
        ORDER BY total_revenue DESC
    """).df()


def revenue_ranking(
    con: duckdb.DuckDBPyConnection,
    fact_table: str,
    dim_table: str,
    join_col: str,
    group_cols: list,
    value_col: str = "total_price",
    extra_metrics: list = None
) -> pd.DataFrame:
    """Receita por grupo com ranking.
    
    extra_metrics — lista de tuplas (agg, col, alias): ex [('AVG', 'reward_points', 'avg_rewards')]
    """
    a      = dim_table.split('_', 1)[1][0]
    group  = ', '.join(f"{a}.{c}" for c in group_cols)
    extras = ""
    if extra_metrics:
        extras = ", " + ", ".join(f"ROUND({agg}(f.{col}), 2) AS {alias}" for agg, col, alias in extra_metrics)

    return con.execute(f"""
        SELECT
            {group},
            COUNT(*)                        AS total_orders,
            ROUND(SUM(f.{value_col}), 2)    AS total_revenue,
            ROUND(AVG(f.{value_col}), 2)    AS avg_ticket
            {extras},
            RANK() OVER (
                ORDER BY SUM(f.{value_col}) DESC
            )                               AS revenue_rank
        FROM {fact_table} f
        LEFT JOIN {dim_table} {a} USING ({join_col})
        GROUP BY {group}
        ORDER BY revenue_rank
    """).df()


def running_total(
    con: duckdb.DuckDBPyConnection,
    fact_table: str,
    dim_table: str,
    join_col: str,
    partition_col: str,
    order_col: str,
    value_col: str,
    label_col: str = None
) -> pd.DataFrame:
    """Total acumulado de uma métrica dentro de cada partição.
    
    label_col — coluna descritiva opcional para exibir junto (ex: product_name)
    """
    a     = dim_table.split('_', 1)[1][0]
    label = f", {a}.{label_col}" if label_col else ""

    return con.execute(f"""
        SELECT
            {a}.{partition_col},
            f.{order_col}
            {label},
            f.{value_col},
            ROUND(SUM(f.{value_col}) OVER (
                PARTITION BY {a}.{partition_col}
                ORDER BY f.{order_col}
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 2)                           AS running_total
        FROM {fact_table} f
        LEFT JOIN {dim_table} {a} USING ({join_col})
        ORDER BY {a}.{partition_col}, f.{order_col}
    """).df()


def lag_analysis(
    con: duckdb.DuckDBPyConnection,
    fact_table: str,
    dim_table: str,
    join_col: str,
    partition_col: str,
    order_col: str,
    value_col: str,
    label_col: str = None
) -> pd.DataFrame:
    """Diferença entre valor atual e anterior dentro de cada partição."""
    a     = dim_table.split('_', 1)[1][0]
    label = f", {a}.{label_col}" if label_col else ""

    return con.execute(f"""
        SELECT
            {a}.{partition_col},
            f.{order_col}
            {label},
            f.{value_col},
            LAG(f.{value_col}) OVER (
                PARTITION BY {a}.{partition_col}
                ORDER BY f.{order_col}
            )                               AS prev_{value_col},
            ROUND(f.{value_col} - LAG(f.{value_col}) OVER (
                PARTITION BY {a}.{partition_col}
                ORDER BY f.{order_col}
            ), 2)                           AS diff
        FROM {fact_table} f
        LEFT JOIN {dim_table} {a} USING ({join_col})
        ORDER BY {a}.{partition_col}, f.{order_col}
    """).df()


def moving_average(
    con: duckdb.DuckDBPyConnection,
    fact_table: str,
    dim_table: str,
    join_col: str,
    partition_col: str,
    order_col: str,
    value_col: str,
    window: int = 3,
    label_col: str = None
) -> pd.DataFrame:
    """Média móvel de uma métrica dentro de cada partição."""
    a     = dim_table.split('_', 1)[1][0]
    label = f", {a}.{label_col}" if label_col else ""

    return con.execute(f"""
        SELECT
            {a}.{partition_col},
            f.{order_col}
            {label},
            f.{value_col},
            ROUND(AVG(f.{value_col}) OVER (
                PARTITION BY {a}.{partition_col}
                ORDER BY f.{order_col}
                ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW
            ), 2)                           AS moving_avg_{window}
        FROM {fact_table} f
        LEFT JOIN {dim_table} {a} USING ({join_col})
        ORDER BY {a}.{partition_col}, f.{order_col}
    """).df()


def save_report(df: pd.DataFrame, path: str, report_name: str) -> None:
    """Salva relatório como CSV."""
    df.to_csv(path, index=False)
    print(f"[report] {report_name} → {path}")


# ==HELPERS INTERNOS==
def _get_columns(con: duckdb.DuckDBPyConnection, table: str) -> list:
    """Retorna lista de colunas de uma tabela."""
    return [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]


def _infer_metric(con: duckdb.DuckDBPyConnection, fact_table: str) -> str:
    """Infere a coluna de métrica principal da fact table (total_price ou equivalente)."""
    cols = _get_columns(con, fact_table)
    for candidate in ["total_price", "revenue", "amount", "value", "total"]:
        if candidate in cols:
            return candidate
    raise ValueError(f"[pipeline] Nenhuma coluna de métrica encontrada em '{fact_table}'. Colunas disponíveis: {cols}")