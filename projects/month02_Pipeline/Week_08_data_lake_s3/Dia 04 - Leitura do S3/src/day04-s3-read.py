import os
import pandas as pd
from dotenv import load_dotenv
from s3_reader import get_client, read_parquet, download_file, list_layer, read_layer

load_dotenv()

BUCKET      = os.getenv("S3_BUCKET",  "de-portfolio-deliveries")
REGION      = os.getenv("AWS_REGION", "us-east-1")
DOWNLOAD_PATH = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month02_Pipeline/Data/lake/downloads"

s3 = get_client(REGION)

print("\n===LISTAR CAMADAS===")
for layer in ["bronze", "silver", "gold"]:
    list_layer(s3, BUCKET, layer)


print("\n===LER SILVER DIRETO DO S3===")
df_silver = read_parquet(s3, BUCKET, "silver/deliveries_clean.parquet")
print(df_silver.head(5))
print(f"\n[info] dtypes:\n{df_silver.dtypes}")


print("\n===LER GOLD COMPLETA===")
gold_dfs = read_layer(s3, BUCKET, "gold")
for name, df in gold_dfs.items():
    print(f"\n[gold] {name}:")
    print(df.head(5))


print("\n===DOWNLOAD BRONZE===")
download_file(s3, BUCKET, "bronze/deliveries.parquet", f"{DOWNLOAD_PATH}/bronze_deliveries.parquet")
download_file(s3, BUCKET, "bronze/drivers.parquet",    f"{DOWNLOAD_PATH}/bronze_drivers.parquet")
download_file(s3, BUCKET, "bronze/hubs.parquet",       f"{DOWNLOAD_PATH}/bronze_hubs.parquet")


print("\n===VALIDAR CONSISTÊNCIA===")
df_s3    = read_parquet(s3, BUCKET, "silver/deliveries_clean.parquet")
df_local = pd.read_parquet(r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month02_Pipeline/Data/lake/silver/deliveries_clean.parquet")


match = len(df_s3) == len(df_local)
print(f"[validate] S3: {len(df_s3)} linhas | local: {len(df_local)} linhas | {'OK' if match else 'DIVERGENTE'}")


print("\n[done] Day 04 concluído")