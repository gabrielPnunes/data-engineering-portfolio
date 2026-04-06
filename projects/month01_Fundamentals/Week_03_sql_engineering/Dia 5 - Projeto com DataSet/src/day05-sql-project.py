#==BLIBIOTECAS E FUNÇÕES==#
from queries import get_connection
from modeling import setup_duckdb, create_dimension, create_fact, aggregate
from pipeline import (
    revenue_ranking,
    running_total,
    lag_analysis,
    moving_average,
    save_report
)

#==CMAINHO DE EXTRAÇÃO E LOAD==#
filepath = r'C:\Users\AMD\Desktop\data-engineering-portfolio\projects\month01_Fundamentals\Data\Raw\sales.csv'
loadpath = r'C:\Users\AMD\Desktop\data-engineering-portfolio\projects\month01_Fundamentals\Data\Staging'

#==CONFIDURAÇÕES==#
TABLE = "sales"
con   = get_connection(filepath, table_name=TABLE)
con   = setup_duckdb(con)

#==CONSTRUÇÃO DO MODELO ESTRELA==#
con = create_dimension(con, TABLE, 'dim_customer', ['customer_type', 'gender'],                        'customer_id')
con = create_dimension(con, TABLE, 'dim_product',  ['product_name', 'product_category', 'unit_price'], 'product_id', order_by=['product_name', 'product_category'])
con = create_dimension(con, TABLE, 'dim_location', ['branch', 'city'],                                 'location_id')

con = create_fact(con, TABLE, 'fact_sales',
    fact_columns=['sale_id', 'quantity', 'tax', 'total_price', 'reward_points'],
    dimensions=[
        {'table': 'dim_product',  'join_on': ['product_name', 'product_category']},
        {'table': 'dim_location', 'join_on': ['branch', 'city']},
        {'table': 'dim_customer', 'join_on': ['customer_type', 'gender']},
    ]
)

#==AGREGAÇÕES, MEDIDAS E SAVALMENTO EM RELATORIOS==#
df_cat = aggregate(con, 'fact_sales', 'dim_product', 'product_id', ['product_category'], 'total_price')
print(df_cat.to_string(index=False))
save_report(df_cat, f"{loadpath}/report_category.csv", "revenue_by_category")

print("\n=== RECEITA POR LOCALIZAÇÃO ===")
df_loc = aggregate(con, 'fact_sales', 'dim_location', 'location_id', ['city', 'branch'], 'total_price')
print(df_loc.to_string(index=False))
save_report(df_loc, f"{loadpath}/report_location.csv", "revenue_by_location")

print("\n=== TICKET MÉDIO POR CATEGORIA ===")
df_avg = aggregate(con, 'fact_sales', 'dim_product', 'product_id', ['product_category'], 'total_price', agg='AVG')
print(df_avg.to_string(index=False))
save_report(df_avg, f"{loadpath}/report_avg_ticket.csv", "avg_ticket_by_category")


#==RANKING
print("\n=== RANKING: CUSTOMER TYPE + GENDER ===")
df_rank = revenue_ranking(
    con, 'fact_sales', 'dim_customer', 'customer_id',
    group_cols=['customer_type', 'gender'],
    value_col='total_price',
    extra_metrics=[('AVG', 'reward_points', 'avg_reward_points')]
)
print(df_rank.to_string(index=False))
save_report(df_rank, f"{loadpath}/report_customer_ranking.csv", "customer_revenue_ranking")

# Running total por categoria
print("\n=== RUNNING REVENUE POR CATEGORIA ===")
df_running = running_total(
    con, 'fact_sales', 'dim_product', 'product_id',
    partition_col='product_category',
    order_col='sale_id',
    value_col='total_price',
    label_col='product_name'
)
print(df_running.head(20).to_string(index=False))
save_report(df_running, f"{loadpath}/report_running_revenue.csv", "running_revenue_by_category")

# Lag analysis por cidade
print("\n=== LAG ANALYSIS POR CIDADE ===")
df_lag = lag_analysis(
    con, 'fact_sales', 'dim_location', 'location_id',
    partition_col='city',
    order_col='sale_id',
    value_col='total_price'
)
print(df_lag.head(20).to_string(index=False))
save_report(df_lag, f"{loadpath}/report_lag_analysis.csv", "lag_analysis_by_city")

# Moving average por branch
print("\n=== MOVING AVERAGE (3) POR BRANCH ===")
df_ma = moving_average(
    con, 'fact_sales', 'dim_location', 'location_id',
    partition_col='branch',
    order_col='sale_id',
    value_col='total_price',
    window=3
)
print(df_ma.head(20).to_string(index=False))
save_report(df_ma, f"{loadpath}/report_moving_avg.csv", "moving_avg_by_branch")

con.close()
print("\n[db] Conexão encerrada")