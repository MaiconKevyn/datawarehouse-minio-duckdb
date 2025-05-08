#!/usr/bin/env python3
import boto3
from botocore.client import Config
import os
from dotenv import load_dotenv


load_dotenv()

# === Configurações do MinIO ===
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET")
S3_PREFIX        = os.getenv("MINIO_S3_PREFIX")
AWS_REGION       = os.getenv("AWS_REGION", "us-east-1")


def main():
    # Conecta ao MinIO
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    # Lista objetos com paginação
    print(f"🔍 Listando objetos em s3://{MINIO_BUCKET}/{S3_PREFIX}")
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=S3_PREFIX):
        contents = page.get('Contents', [])
        if not contents:
            print("Nenhum objeto encontrado nesse prefixo.")
            return
        for obj in contents:
            print(f"- {obj['Key']} (tamanho: {obj['Size']} bytes)")

if __name__ == '__main__':
    main()
