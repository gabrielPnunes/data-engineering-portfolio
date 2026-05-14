import os
import boto3
import pandas as pd
from io import BytesIO
from pathlib import Path
from botocore.exceptions import ClientError


def get_client(region: str = "us-east-1"):
    return boto3.client(
        "s3",
        region_name           = region,
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def read_parquet(client, bucket: str, key: str) -> pd.DataFrame:
    try:
        resp   = client.get_object(Bucket=bucket, Key=key)
        buffer = BytesIO(resp["Body"].read())
        df     = pd.read_parquet(buffer)
        print(f"[read] s3://{bucket}/{key} - {len(df)} linhas, {len(df.columns)} colunas")
        return df
    except ClientError as e:
        raise RuntimeError(f"[error] falha ao ler {key}: {e}")


def read_csv(client, bucket: str, key: str, **kwargs) -> pd.DataFrame:
    try:
        resp   = client.get_object(Bucket=bucket, Key=key)
        buffer = BytesIO(resp["Body"].read())
        df     = pd.read_csv(buffer, **kwargs)
        print(f"[read] s3://{bucket}/{key} - {len(df)} linhas, {len(df.columns)} colunas")
        return df
    except ClientError as e:
        raise RuntimeError(f"[error] falha ao ler {key}: {e}")


def download_file(client, bucket: str, key: str, local_path: str) -> str:
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    client.download_file(bucket, key, local_path)
    size = round(Path(local_path).stat().st_size / 1024, 2)
    print(f"[download] s3://{bucket}/{key} → {local_path} | {size} KB")
    return local_path


def list_layer(client, bucket: str, layer: str) -> list:
    resp    = client.list_objects_v2(Bucket=bucket, Prefix=f"{layer}/")
    objects = [obj["Key"] for obj in resp.get("Contents", [])]
    print(f"[list] camada '{layer}' - {len(objects)} objeto(s)")
    for key in objects:
        print(f"  s3://{bucket}/{key}")
    return objects


def read_layer(client, bucket: str, layer: str) -> dict:
    keys = list_layer(client, bucket, layer)
    dfs  = {}
    for key in keys:
        if not key.endswith(".parquet"):
            continue
        name      = Path(key).stem
        dfs[name] = read_parquet(client, bucket, key)
    print(f"[read] {len(dfs)} DataFrame(s) carregado(s) da camada '{layer}'")
    return dfs