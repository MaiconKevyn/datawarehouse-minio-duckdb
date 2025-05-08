#!/usr/bin/env python3
import os
import io
import pandas as pd
import boto3
from botocore.client import Config

def load_sih_dataframe():
    """
    Conecta ao MinIO, faz listagem do prefixo SIH, baixa
    cada arquivo Parquet em memória e retorna um único DataFrame.
    """
    ENDPOINT   = os.environ["MINIO_ENDPOINT"]
    ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
    SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
    BUCKET     = os.environ["MINIO_BUCKET"]
    PREFIX     = os.environ["MINIO_S3_PREFIX"]
    REGION     = os.getenv("AWS_REGION", "us-east-1")

    s3 = boto3.client(
        's3',
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name=REGION
    )

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX)

    dfs = []
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            print(f"🔄 Carregando {key}...")
            data = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
            buf = io.BytesIO(data)
            dfs.append(pd.read_parquet(buf))

    if not dfs:
        raise RuntimeError(f"Nenhum Parquet encontrado em s3://{BUCKET}/{PREFIX}")

    return pd.concat(dfs, ignore_index=True)