#!/usr/bin/env python3
import os
import io
import pandas as pd
from pysus import SIH
import boto3
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

# === Configurações do MinIO ===
ENDPOINT = os.environ["MINIO_ENDPOINT"]
ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
BUCKET = os.environ["MINIO_BUCKET"]
PREFIX = os.environ["MINIO_S3_PREFIX"]
REGION = os.getenv("AWS_REGION", "us-east-1")


def main():
    # 1) Carrega metadados e lista arquivos DBC
    print("Carregando SIH metadata...")
    sih = SIH().load()
    files = sih.get_files(
        group="RD",    # RD = AIH Reduzida
        uf="SP",
        year=2020,
        month=[1,2,3]
    )
    print(f"🔍 {len(files)} arquivos encontrados.")

    # 2) Converte DBC → Parquet (em /tmp)
    tmp_dir = "/tmp/sih_parquets"
    os.makedirs(tmp_dir, exist_ok=True)
    print("Convertendo DBC → Parquet (em /tmp)...")
    parquet_sets = sih.download(files, local_dir=tmp_dir)
    print(f"Geração de {len(parquet_sets)} Parquets concluída.")

    # 3) Conecta ao MinIO
    s3 = boto3.client(
        's3',
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    # cria bucket se não existir
    try:
        s3.head_bucket(Bucket=BUCKET)
    except:
        print(f"Bucket '{BUCKET}' não existe. Criando…")
        s3.create_bucket(Bucket=BUCKET)

    # 4) Loop de upload e limpeza
    for pf in parquet_sets:
        # Trata pf como objeto e converte em path string
        path_str = str(pf)
        filename = os.path.basename(path_str)
        s3_key = f"{PREFIX}{filename}"
        print(f"Upload de {filename} → s3://{BUCKET}/{s3_key}")

        # Lê Parquet direto em DataFrame
        df = pd.read_parquet(path_str)

        # Empacota em buffer e envia
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        s3.upload_fileobj(buf, BUCKET, s3_key)
        print("   OK.")

        # Remove arquivo temporário
        try:
            os.remove(path_str)
        except Exception:
            pass

    print("Todos os arquivos foram enviados e limpos de /tmp.")

if __name__ == "__main__":
    main()
