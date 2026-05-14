import os
import boto3
from pathlib import Path
from botocore.exceptions import ClientError


def get_client(region: str = "us-east-1"):
    return boto3.client(
        "s3",
        region_name           = region,
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def upload_file(client, local_path: str, bucket: str, key: str, metadata: dict = None) -> bool:
    extra = {"Metadata": {k: str(v) for k, v in metadata.items()}} if metadata else {}
    try:
        client.upload_file(local_path, bucket, key, ExtraArgs=extra)
        size = round(Path(local_path).stat().st_size / 1024, 2)
        print(f"[upload] {key} - {size} KB")
        return True
    except ClientError as e:
        print(f"[error] falha ao fazer upload de {local_path}: {e}")
        return False


def upload_folder(client, local_path: str, bucket: str, prefix: str) -> list:
    uploaded = []
    for f in sorted(Path(local_path).rglob("*")):
        if not f.is_file():
            continue
        key = f"{prefix}/{f.name}"
        if upload_file(client, str(f), bucket, key):
            uploaded.append(key)
    print(f"[upload] {len(uploaded)} arquivo(s) enviado(s) para '{prefix}/'")
    return uploaded


def list_objects(client, bucket: str, prefix: str = "") -> list:
    resp    = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = resp.get("Contents", [])
    print(f"[s3] {len(objects)} objeto(s) em '{bucket}/{prefix}'")
    for obj in objects:
        size = round(obj["Size"] / 1024, 2)
        print(f"  {obj['Key']} - {size} KB")
    return objects


def object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False
    
    
def get_object_metadata(client, bucket: str, key: str) -> dict:
    resp = client.head_object(Bucket=bucket, Key=key)
    meta = resp.get("Metadata", {})
    print(f"[s3] metadata de '{key}': {meta}")
    return meta