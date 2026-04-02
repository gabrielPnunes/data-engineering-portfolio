#==BIBLIOTECA==#
from queries import get_connection
from joins import (
    inner_join,
    left_join,
    right_join,
    full_outer_join,
    join_with_filter,
)

#==CAMINHO==#
filepath = r'C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.csv'

#==CONEXÃO==#
con = get_connection(filepath, table_name="sales")

#==VIEWS DE MEMBROS==#
con.execute("""
    CREATE OR REPLACE VIEW sales_member AS
    SELECT
        product_name,
        product_category,
        city,
        branch,
        COUNT(*)                   AS orders_member,
        ROUND(SUM(total_price), 2) AS revenue_member,
        ROUND(AVG(total_price), 2) AS avg_ticket_member,
        SUM(reward_points)         AS points_member
    FROM sales
    WHERE customer_type = 'Member'
    GROUP BY product_name, product_category, city, branch
""")

#==VIEWS DE NORMAL==#
con.execute("""
    CREATE OR REPLACE VIEW sales_normal AS
    SELECT
        product_name,
        product_category,
        city,
        branch,
        COUNT(*)                   AS orders_normal,
        ROUND(SUM(total_price), 2) AS revenue_normal,
        ROUND(AVG(total_price), 2) AS avg_ticket_normal
    FROM sales
    WHERE customer_type = 'Normal'
    GROUP BY product_name, product_category, city, branch
""")

#==CHAVE DO JOIN==#
chave = ["product_name", "product_category", "city", "branch"]

#==INNER JOIN==#
print("\n ====INNER JOIN: produtos com ambos os tipos de cliente==== ")
print(inner_join(
    con,
    left="sales_member",
    right="sales_normal",
    on=chave,
    columns=[
        "sales_member.product_name",
        "sales_member.product_category",
        "sales_member.city",
        "sales_member.branch",
        "sales_member.orders_member",
        "sales_normal.orders_normal",
        "sales_member.revenue_member",
        "sales_normal.revenue_normal",
    ],
))

#==LEFT JOIN==#
print("\n ====LEFT JOIN: todos os Member, com Normal onde existir==== ")
print(left_join(
    con,
    left="sales_member",
    right="sales_normal",
    on=chave,
    columns=[
        "sales_member.product_name",
        "sales_member.product_category",
        "sales_member.city",
        "sales_member.revenue_member",
        "sales_normal.revenue_normal",
        "sales_member.points_member",
    ],
))

#==RIGHT JOIN==#
print("\n ====RIGHT JOIN: todos os Normal, com Member onde existir==== ")
print(right_join(
    con,
    left="sales_member",
    right="sales_normal",
    on=chave,
    columns=[
        "sales_normal.product_name",
        "sales_normal.product_category",
        "sales_normal.city",
        "sales_member.revenue_member",
        "sales_normal.revenue_normal",
        "sales_normal.avg_ticket_normal",
    ],
))

#==FULL OUTER JOIN==#
print("\n ====FULL OUTER JOIN: visão completa Member vs Normal==== ")
print(full_outer_join(
    con,
    left="sales_member",
    right="sales_normal",
    on=chave,
))

#==LEFT JOIN COM FILTER==#
print("\n ====LEFT JOIN + FILTER: city = 'New York'==== ")
print(join_with_filter(
    con,
    left="sales_member",
    right="sales_normal",
    on=chave,
    filter_col="city",
    filter_val="New York",
    columns=[
        "sales_member.product_name",
        "sales_member.product_category",
        "sales_member.city",
        "sales_member.revenue_member",
        "sales_normal.revenue_normal",
    ],
))

#==INNER JOIN COM FILTER==#
print("\n ====INNER JOIN + FILTER: product_category = 'Beverages'==== ")
print(join_with_filter(
    con,
    left="sales_member",
    right="sales_normal",
    on=chave,
    filter_col="product_category",
    filter_val="Beverages",
    join_type="INNER",
    columns=[
        "sales_member.product_name",
        "sales_member.product_category",
        "sales_member.city",
        "sales_member.avg_ticket_member",
        "sales_normal.avg_ticket_normal",
        "sales_member.points_member",
    ],
))