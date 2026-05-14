import boto3
import os
from botocore.exceptions import ClientError, NoCredentialsError

region = os.getenv("AWS_REGION", "us-east-1")

client = boto3.client(
    "s3",
    region_name            = region,
    aws_access_key_id      = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key  = os.getenv("AWS_SECRET_ACCESS_KEY"),
)

def get_client(region: str = "us-east-1"):
    try:
        client = boto3.client("s3", region_name=region)
        client.list_buckets()
        print(f"[s3] cliente autenticado - região: {region}")
        return client
    except NoCredentialsError:
        raise RuntimeError("[s3] credenciais não encontradas - configure ~/.aws/credentials ou variáveis de ambiente")
    except Exception as e:
        raise RuntimeError(f"[s3] falha ao autenticar: {e}")


def create_bucket(client, bucket: str, region: str = "us-east-1") -> bool:
    try:
        if region == "us-east-1":
            client.create_bucket(Bucket=bucket)
        else:
            client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
        print(f"[s3] bucket criado: {bucket}")
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"[s3] bucket já existe: {bucket}")
            return True
        raise RuntimeError(f"[s3] erro ao criar bucket: {e}")


def bucket_exists(client, bucket: str) -> bool:
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except ClientError:
        return False


def list_buckets(client) -> list:
    resp    = client.list_buckets()
    buckets = [b["Name"] for b in resp.get("Buckets", [])]
    print(f"[s3] {len(buckets)} bucket(s) encontrado(s)")
    for b in buckets:
        print(f"  - {b}")
    return buckets


def set_lifecycle(client, bucket: str, days: int = 90) -> None:
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [{
                "ID":     "expire-old-objects",
                "Status": "Enabled",
                "Filter": {"Prefix": ""},
                "Expiration": {"Days": days}
            }]
        }
    )
    print(f"[s3] lifecycle configurado - objetos expiram após {days} dias")


def get_bucket_info(client, bucket: str) -> dict:
    region   = client.get_bucket_location(Bucket=bucket)["LocationConstraint"] or "us-east-1"
    try:
        lifecycle = client.get_bucket_lifecycle_configuration(Bucket=bucket)
        rules     = len(lifecycle.get("Rules", []))
    except ClientError:
        rules = 0

    info = {"bucket": bucket, "region": region, "lifecycle_rules": rules}
    print(f"[s3] info - {info}")
    return info

def delete_bucket(client, bucket: str, confirm: bool = False) -> None:
    if not confirm:
        print(f"[s3] passe confirm=True para deletar '{bucket}'")
        return
    client.delete_bucket(Bucket=bucket)
    print(f"[s3] bucket deletado: {bucket}")