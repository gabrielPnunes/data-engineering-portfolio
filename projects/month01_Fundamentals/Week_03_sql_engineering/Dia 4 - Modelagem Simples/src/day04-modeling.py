from queries import get_connection
from modeling import setup_duckdb, create_dimension, create_fact, aggregate

filepath = r'C:\Users\AMD\Desktop\data-engineering-portfolio\projects\month01_Fundamentals\Data\Raw\sales.csv'

con = get_connection(filepath, table_name="sales")
con = setup_duckdb(con)

con = create_dimension(con, 'sales', 'dim_customer', ['customer_type', 'gender'],                        'customer_id')
con = create_dimension(con, 'sales', 'dim_product',  ['product_name', 'product_category', 'unit_price'], 'product_id', order_by=['product_name', 'product_category'])
con = create_dimension(con, 'sales', 'dim_location', ['branch', 'city'],                                'location_id')

con = create_fact(con, 'sales', 'fact_sales',
    fact_columns=['sale_id', 'quantity', 'tax', 'total_price', 'reward_points'],
    dimensions=[
        {'table': 'dim_product',  'join_on': ['product_name', 'product_category']},
        {'table': 'dim_location', 'join_on': ['branch', 'city']},
        {'table': 'dim_customer', 'join_on': ['customer_type', 'gender']},
    ]
)

print("\n==== Receita por categoria ====")
print(aggregate(con, 'fact_sales', 'dim_product',  'product_id',  ['product_category'],       'total_price').to_string(index=False))

print("\n==== Receita por localização ====")
print(aggregate(con, 'fact_sales', 'dim_location', 'location_id', ['city', 'branch'],          'total_price').to_string(index=False))

print("\n==== Receita por tipo de cliente ====")
print(aggregate(con, 'fact_sales', 'dim_customer', 'customer_id', ['customer_type', 'gender'], 'total_price').to_string(index=False))