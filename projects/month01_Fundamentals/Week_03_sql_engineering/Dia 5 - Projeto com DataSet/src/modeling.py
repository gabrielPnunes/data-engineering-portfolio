import pandas as pd
from queries import get_connection

filepath = r'C:\Users\AMD\Desktop\data-engineering-portfolio\projects\month01_Fundamentals\Data\Raw\sales.csv'


def setup_duckdb(con, memory_limit='1GB', threads=2):
    con.execute(f"PRAGMA memory_limit='{memory_limit}';")
    con.execute(f"PRAGMA threads={threads};")
    con.execute("SET preserve_insertion_order=false;")
    return con


def create_dimension(con, source_table, dim_table, columns, id_column, order_by=None):
    order_by = order_by or columns
    cols     = ', '.join(columns)

    con.execute(f"""
        CREATE OR REPLACE TABLE {dim_table} AS
        SELECT
            row_number() OVER (ORDER BY {', '.join(order_by)}) AS {id_column},
            {cols}
        FROM (SELECT DISTINCT {cols} FROM {source_table})
    """)
    print(f"[dim] {dim_table} — {con.execute(f'SELECT count(*) FROM {dim_table}').fetchone()[0]} registros.")
    return con


def create_fact(con, source_table, fact_table, fact_columns, dimensions):
    found = {r[0] for r in con.execute(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_name IN ({', '.join(f"'{d['table']}'" for d in dimensions)})
    """).fetchall()}

    missing = {d['table'] for d in dimensions} - found
    if missing:
        raise RuntimeError(f"[fact] Dimensões não encontradas: {missing}")

    def alias(d):
        return d['table'].split('_', 1)[1][0]

    def id_col(d):
        return f"{d['table'].split('_', 1)[1]}_id"

    con.execute(f"""
        CREATE OR REPLACE TABLE {fact_table} AS
        SELECT
            {', '.join(fact_columns)},
            {', '.join(f"{alias(d)}.{id_col(d)}" for d in dimensions)}
        FROM {source_table}
        {chr(10).join(f"LEFT JOIN {d['table']} {alias(d)} USING ({', '.join(d['join_on'])})" for d in dimensions)}
    """)

    total  = con.execute(f"SELECT count(*) FROM {fact_table}").fetchone()[0]
    nulls  = {id_col(d): con.execute(f"SELECT COUNT(*) FILTER (WHERE {id_col(d)} IS NULL) FROM {fact_table}").fetchone()[0] for d in dimensions}
    print(f"[fact] {fact_table} — {total} registros | FKs nulas: {nulls}")
    return con


def aggregate(con, fact_table, dim_table, join_on, group_columns, metric_column, agg='SUM', limit=10):
    a     = dim_table.split('_', 1)[1][0]
    group = ', '.join(f"{a}.{c}" for c in group_columns)

    return con.execute(f"""
        SELECT
            {group},
            COUNT(*) AS total_orders,
            ROUND({agg}(f.{metric_column}), 2) AS {agg.lower()}_{metric_column}
        FROM {fact_table} f
        LEFT JOIN {dim_table} {a} USING ({join_on})
        GROUP BY {group}
        ORDER BY {agg.lower()}_{metric_column} DESC
        LIMIT {limit}
    """).df()


if __name__ == "__main__":
    con = get_connection(filepath, table_name="sales")
    con = setup_duckdb(con)

    con = create_dimension(con, 'sales', 'dim_customer', ['customer_type', 'gender'],              'customer_id')
    con = create_dimension(con, 'sales', 'dim_product',  ['product_name', 'product_category', 'unit_price'], 'product_id', order_by=['product_name', 'product_category'])
    con = create_dimension(con, 'sales', 'dim_location', ['branch', 'city'],                       'location_id')

    con = create_fact(con, 'sales', 'fact_sales',
        fact_columns=['sale_id', 'quantity', 'tax', 'total_price', 'reward_points'],
        dimensions=[
            {'table': 'dim_product',  'join_on': ['product_name', 'product_category']},
            {'table': 'dim_location', 'join_on': ['branch', 'city']},
            {'table': 'dim_customer', 'join_on': ['customer_type', 'gender']},
        ]
    )

    print(aggregate(con, 'fact_sales', 'dim_product',  'product_id',  ['product_category'],       'total_price'))
    print(aggregate(con, 'fact_sales', 'dim_location', 'location_id', ['city', 'branch'],          'total_price'))
    print(aggregate(con, 'fact_sales', 'dim_customer', 'customer_id', ['customer_type', 'gender'], 'total_price'))
    print(aggregate(con, 'fact_sales', 'dim_product',  'product_id',  ['product_category'],        'total_price', agg='AVG'))