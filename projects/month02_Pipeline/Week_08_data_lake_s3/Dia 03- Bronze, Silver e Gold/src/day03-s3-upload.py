import os
from pathlib import Path
from dotenv import load_dotenv
from s3_uploader import get_client, upload_file, upload_folder, list_objects, object_exists

load_dotenv()

BUCKET    = os.getenv("S3_BUCKET",  "de-portfolio-deliveries")
REGION    = os.getenv("AWS_REGION", "us-east-1")
LAKE_PATH = r"C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month02_Pipeline/Data/lake"

s3 = get_client(REGION)

print("\n===UPLOAD BRONZE===")
upload_folder(s3, f"{LAKE_PATH}/bronze", BUCKET, "bronze")


print("\n===UPLOAD SILVER===")
upload_folder(s3, f"{LAKE_PATH}/silver", BUCKET, "silver")


print("\n===UPLOAD GOLD===")
upload_folder(s3, f"{LAKE_PATH}/gold", BUCKET, "gold")


print("\n===UPLOAD COM METADATA===")
silver_file = str(Path(LAKE_PATH) / "silver" / "deliveries_clean.parquet")
upload_file(s3, silver_file, BUCKET, "silver/deliveries_clean_v2.parquet", metadata={
    "source":    "deliveries.csv",
    "layer":     "silver",
    "rows":      "33951",
    "version":   "2",
})


print("\n===VALIDAR===")
for key in ["bronze/deliveries.parquet", "silver/deliveries_clean.parquet", "gold/deliveries_by_hub.parquet"]:
    exists = object_exists(s3, BUCKET, key)
    print(f"[validate] {key} - {'OK' if exists else 'NÃO ENCONTRADO'}")

print("\n===OBJETOS NO S3===")
for layer in ["bronze", "silver", "gold"]:
    list_objects(s3, BUCKET, prefix=layer)

print("\n[done] Day 03 concluído")
