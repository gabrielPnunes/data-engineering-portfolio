import os
from dotenv import load_dotenv
from s3_client import (
    get_client,
    create_bucket,
    bucket_exists,
    list_buckets,
    set_lifecycle,
    get_bucket_info
)

load_dotenv()

BUCKET = os.getenv("S3_BUCKET", "de-portfolio-deliveries")
REGION = os.getenv("AWS_REGION", "us-east-1")

print("\n===AUTENTICAR===")
s3 = get_client(REGION)

print("\n===BUCKETS EXISTENTES===")
list_buckets(s3)

print("\n===CRIAR BUCKET===")
create_bucket(s3, BUCKET, REGION)

print("\n===VALIDAR===")
exists = bucket_exists(s3, BUCKET)
print(f"[validate] bucket '{BUCKET}' existe: {exists}")

print("\n===LIFECYCLE===")
set_lifecycle(s3, BUCKET, days=90)

print("\n===INFO===")
get_bucket_info(s3, BUCKET)

print("\n===BUCKETS PÓS CRIAÇÃO===")
list_buckets(s3)