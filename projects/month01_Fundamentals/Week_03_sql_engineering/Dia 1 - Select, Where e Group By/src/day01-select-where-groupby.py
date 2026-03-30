#====BIBLIOTECA====#
from queries import (
    get_connection,
    select_columns,
    filter_by_column,
    filter_by_value,
    group_by_column,
    group_by_multiple_columns,
    top_n_by_column
)

#====CAMINHO====#
filepath = r'C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.csv'

#====EXECUÇÃO====#
TABLE = "orders"

con = get_connection(filepath, TABLE)

print("\n ====SELECT====")
df_select = select_columns(con, TABLE, [
    "sale_id", "branch", "city", "customer_type", "gender",
    "product_name", "product_category", "unit_price",
    "quantity", "tax", "total_price", "reward_points"
])
print(df_select.head(10))

print("\n ====WHERE branch IN ('A', 'B')==== ")
df_branch = filter_by_column(con, TABLE, "branch", ["A", "B"])
print(df_branch)


print("\n =====WHERE customer_type = 'Member'==== ")
df_member = filter_by_column(con, TABLE, "customer_type", ["Member"])
print(df_member)

print("\n ====WHERE total_price > 100==== ")
df_price = filter_by_value(con, TABLE, "total_price", 100, ">")
print(df_price.head(20))

print("\n ====WHERE reward_points >= 20===")
df_rewards = filter_by_value(con, TABLE, "reward_points", 20, ">=")
print(df_rewards.head(20))

print("\n ====GROUP BY product_category==== ")
df_category = group_by_column(con, TABLE, "product_category", "total_price")
print(df_category)

print("\ ====GROUP BY city==== ")
df_city = group_by_column(con, TABLE, "city", "total_price")
print(df_city)

print("\n ====GROUP BY branch===== ")
df_branch_group = group_by_column(con, TABLE, "branch", "total_price")
print(df_branch_group)

print("\n ====GROUP BY branch + product_category==== ")
df_multi = group_by_multiple_columns(con, TABLE, ["branch", "product_category"], "total_price")
print(df_multi)

print("\n ====GROUP BY city + customer_type==== ")
df_multi2 = group_by_multiple_columns(con, TABLE, ["city", "customer_type"], "total_price")
print(df_multi2)

print("\n ====TOP 10 produtos==== ")
df_top_products = top_n_by_column(con, TABLE, "product_name", "total_price", top_n=10)
print(df_top_products)

print("\n ====TOP 5 cidades==== ")
df_top_cities = top_n_by_column(con, TABLE, "city", "total_price", top_n=5)
print(df_top_cities)

con.close()
print("\n[db] Conexão encerrada")