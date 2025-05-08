#!/usr/bin/env python3
import os
import io
import pandas as pd
import boto3
from botocore.client import Config

# Carrega configuração do MinIO via variáveis de ambiente
ENDPOINT   = os.environ["MINIO_ENDPOINT"]  # ex: http://minio:9000
ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
BUCKET     = os.environ["MINIO_BUCKET"]
PREFIX     = os.environ["MINIO_S3_PREFIX"]
REGION     = os.getenv("AWS_REGION", "us-east-1")

# Cria cliente S3/MinIO
s3 = boto3.client(
    's3',
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name=REGION
)

# Lista objetos no prefixo
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX)

# Coleta dataframes em lista
dfs = []
for page in pages:
    for obj in page.get('Contents', []):
        key = obj['Key']
        print(f"🔄 Carregando {key}...")
        response = s3.get_object(Bucket=BUCKET, Key=key)
        data = response['Body'].read()
        buf = io.BytesIO(data)
        df_part = pd.read_parquet(buf)
        dfs.append(df_part)

# Concatena todos os DataFrames em um só
if dfs:
    df = pd.concat(dfs, ignore_index=True)
    print("\n📋 DataFrame concatenado:")
    print(df)
else:
    print("Nenhum arquivo Parquet encontrado no bucket/prefixo.")
