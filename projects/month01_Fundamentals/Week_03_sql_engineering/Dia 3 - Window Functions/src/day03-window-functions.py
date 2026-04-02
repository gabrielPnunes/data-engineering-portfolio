# src/day03-window-functions.py

from queries import get_connection
from window_functions import (
    row_number_by_partition,
    rank_by_partition,
    dense_rank_by_partition,
    lag_column,
    lead_column,
    running_total,
    moving_average
)

filepath = r'C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.csv'

TABLE = "sales"
con   = get_connection(filepath, TABLE)

# ROW NUMBER — numeração sequencial por branch
print("\n=== ROW NUMBER: por branch ordenado por total_price ===")
df_row = row_number_by_partition(con, TABLE, "branch", "total_price")
print(df_row[["branch", "city", "product_name", "total_price", "row_num"]].head(15))

# RANK — ranking de total_price dentro de cada city
print("\n=== RANK: total_price por city ===")
df_rank = rank_by_partition(con, TABLE, "city", "total_price")
print(df_rank[["city", "product_name", "product_category", "total_price", "rank"]].head(15))

# DENSE RANK — ranking sem lacunas por product_category
print("\n=== DENSE RANK: total_price por product_category ===")
df_dense = dense_rank_by_partition(con, TABLE, "product_category", "total_price")
print(df_dense[["product_category", "product_name", "total_price", "dense_rank"]].head(15))

# LAG — valor da venda anterior dentro de cada city
print("\n=== LAG: total_price anterior por city ===")
df_lag = lag_column(con, TABLE, "city", "sale_id", "total_price", offset=1)
print(df_lag[["city", "sale_id", "product_name", "total_price", "prev_total_price"]].head(15))

# LEAD — valor da próxima venda dentro de cada city
print("\n=== LEAD: próximo total_price por city ===")
df_lead = lead_column(con, TABLE, "city", "sale_id", "total_price", offset=1)
print(df_lead[["city", "sale_id", "product_name", "total_price", "next_total_price"]].head(15))

# RUNNING TOTAL — acumulado de total_price por product_category
print("\n=== RUNNING TOTAL: total_price acumulado por product_category ===")
df_running = running_total(con, TABLE, "product_category", "sale_id", "total_price")
print(df_running[["product_category", "sale_id", "product_name", "total_price", "running_total"]].head(15))

# MOVING AVERAGE — média móvel de 3 vendas por branch
print("\n=== MOVING AVERAGE (3): total_price por branch ===")
df_ma3 = moving_average(con, TABLE, "branch", "sale_id", "total_price", window=3)
print(df_ma3[["branch", "sale_id", "product_name", "total_price", "moving_avg_3"]].head(15))

# MOVING AVERAGE — média móvel de 5 vendas por branch
print("\n=== MOVING AVERAGE (5): total_price por branch ===")
df_ma5 = moving_average(con, TABLE, "branch", "sale_id", "total_price", window=5)
print(df_ma5[["branch", "sale_id", "product_name", "total_price", "moving_avg_5"]].head(15))

con.close()
print("\n[db] Conexão encerrada")