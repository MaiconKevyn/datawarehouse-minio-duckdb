#!/usr/bin/env python3
import os
from io import BytesIO

import boto3
from botocore.client import Config
import pandas as pd
from pysus.online_data import SIH, SIASUS  # importe outras classes que precisar

# --- 1. Configuração MinIO via env vars ---
MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY  = os.getenv("Rr0WbrMzpdKUbbQAABHU", "minioadmin")
MINIO_SECRET_KEY  = os.getenv("sDXOiDjjntr5lHUJ9Q9nCMXRS3PRzliTLmTBBN0r", "minioadmin")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET", "pysus-raw")

# Cria cliente boto3 apontando pro MinIO
s3 = boto3.client(
    "s3",
    endpoint_url    = MINIO_ENDPOINT,
    aws_access_key_id     = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    config          = Config(signature_version="s3v4"),
    region_name     = "us-east-1"       # pode ser qualquer string
)

# Garante que o bucket exista
def ensure_bucket(bucket_name: str):
    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception:
        print(f"Bucket '{bucket_name}' não existe. Criando…")
        s3.create_bucket(Bucket=bucket_name)
    else:
        print(f"Bucket '{bucket_name}' já existe.")

# Função genérica para enviar DataFrame como Parquet
def upload_parquet(df: pd.DataFrame, key: str):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=buffer.getvalue())
    print(f"[✔] Uploaded {key}")

# --- 2. Funções de extração PySUS ---
def extract_sih(year: int, month: int):
    print(f"Extraindo SIH {year:04d}-{month:02d} …")
    df = SIH(year=year, month=month).read()
    key = f"sih/year={year:04d}/month={month:02d}/sih_{year:04d}_{month:02d}.parquet"
    upload_parquet(df, key)

def extract_siasus(year: int, month: int):
    print(f"Extraindo SIASUS {year:04d}-{month:02d} …")
    df = SIASUS(year=year, month=month).read()
    key = f"siasus/year={year:04d}/month={month:02d}/siasus_{year:04d}_{month:02d}.parquet"
    upload_parquet(df, key)

# --- 3. Main: itere sobre o que quiser baixar ---
if __name__ == "__main__":
    ensure_bucket(MINIO_BUCKET)

    # Defina aqui as combinações ano/mês que quer baixar:
    jobs = [
        {"fn": extract_sih,     "params": {"year": 2023, "month": 1}},
        {"fn": extract_sih,     "params": {"year": 2023, "month": 2}},
        {"fn": extract_siasus,  "params": {"year": 2023, "month": 1}},
        # …adapte com outros anos/meses e outras funções PySUS
    ]

    for job in jobs:
        try:
            job["fn"](**job["params"])
        except Exception as e:
            print(f"[✘] Falha em {job['fn'].__name__} {job['params']}: {e}")