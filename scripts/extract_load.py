#!/usr/bin/env python3
import os
import io
import pandas as pd
from pysus import SIH
import boto3
from botocore.client import Config

# === Configurações do MinIO ===
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "5FpFEeASYI2j9SBT8QEk"
MINIO_SECRET_KEY = "gyltaePAajfeYZFQWe1TXbHbVEiqBR4UoV3DvDeS"
MINIO_BUCKET     = "davintwarehouse"
S3_PREFIX        = "testes/sih/" # dentro do container, volume ./scripts


def main():
    # 1) Carrega metadados e lista arquivos DBC
    print("🚀 Carregando SIH metadata...")
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
    print("🔄 Convertendo DBC → Parquet (em /tmp)...")
    parquet_sets = sih.download(files, local_dir=tmp_dir)
    print(f"✅ Geração de {len(parquet_sets)} Parquets concluída.")

    # 3) Conecta ao MinIO
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    # cria bucket se não existir
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except:
        print(f"Bucket '{MINIO_BUCKET}' não existe. Criando…")
        s3.create_bucket(Bucket=MINIO_BUCKET)

    # 4) Loop de upload e limpeza
    for pf in parquet_sets:
        # Trata pf como objeto e converte em path string
        path_str = str(pf)
        filename = os.path.basename(path_str)
        s3_key = f"{S3_PREFIX}{filename}"
        print(f"📤 Upload de {filename} → s3://{MINIO_BUCKET}/{s3_key}")

        # Lê Parquet direto em DataFrame
        df = pd.read_parquet(path_str)

        # Empacota em buffer e envia
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        s3.upload_fileobj(buf, MINIO_BUCKET, s3_key)
        print("   OK.")

        # Remove arquivo temporário
        try:
            os.remove(path_str)
        except Exception:
            pass

    print("🎉 Todos os arquivos foram enviados e limpos de /tmp.")

if __name__ == "__main__":
    main()
