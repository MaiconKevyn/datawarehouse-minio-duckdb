#!/usr/bin/env python3
import os
import duckdb
import boto3
from botocore.client import Config

# Configuração do MinIO
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "5FpFEeASYI2j9SBT8QEk"
MINIO_SECRET_KEY = "gyltaePAajfeYZFQWe1TXbHbVEiqBR4UoV3DvDeS"
MINIO_BUCKET = "davintwarehouse"

# Inicializa cliente S3 para MinIO
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)


# Cria bucket se não existir
def ensure_bucket(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' já existe.")
    except Exception:
        print(f"Criando bucket '{bucket_name}'...")
        s3.create_bucket(Bucket=bucket_name)


# Inicializa conexão DuckDB
con = duckdb.connect(database=':memory:')


# Exemplo 1: Criar dados com SQL no DuckDB e exportar para Parquet no MinIO
def create_sample_data():
    print("Criando dados de exemplo...")

    # Criar tabela de exemplo
    con.execute("""
                CREATE TABLE pessoas AS
                SELECT *
                FROM (VALUES (1, 'Ana', 28),
                             (2, 'Bruno', 35),
                             (3, 'Carla', 42),
                             (4, 'Daniel', 19),
                             (5, 'Elena', 31)) AS t(id, nome, idade)
                """)

    # Verificar dados
    result = con.execute("SELECT * FROM pessoas").fetchall()
    print("Dados criados:", result)

    # Exportar para Parquet no MinIO
    con.execute(f"""
    COPY (SELECT * FROM pessoas) TO 's3://{MINIO_BUCKET}/pessoas.parquet' (
        FORMAT 'PARQUET',
        S3_ENDPOINT='{MINIO_ENDPOINT}',
        S3_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}',
        S3_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}',
        S3_URL_STYLE='path'
    )
    """)
    print("Dados exportados para MinIO como Parquet")


# Exemplo 2: Ler um CSV e salvar no MinIO
def process_csv_to_minio():
    # Criar arquivo CSV de exemplo
    with open("/tmp/dados.csv", "w") as f:
        f.write("produto,preco,quantidade\n")
        f.write("Notebook,3500,10\n")
        f.write("Monitor,1200,15\n")
        f.write("Mouse,80,50\n")
        f.write("Teclado,150,30\n")

    # Ler CSV com DuckDB
    con.execute("CREATE TABLE produtos AS SELECT * FROM read_csv_auto('/tmp/dados.csv')")

    # Processar dados (exemplo: calcular valor total)
    con.execute(
        "CREATE TABLE produtos_total AS SELECT produto, preco, quantidade, preco*quantidade AS valor_total FROM produtos")

    # Salvar resultado no MinIO
    con.execute(f"""
    COPY (SELECT * FROM produtos_total) TO 's3://{MINIO_BUCKET}/produtos_processados.parquet' (
        FORMAT 'PARQUET',
        S3_ENDPOINT='{MINIO_ENDPOINT}',
        S3_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}',
        S3_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}',
        S3_URL_STYLE='path'
        
    )
    """)
    print("Produtos processados e exportados para MinIO")


# Executar exemplos
if __name__ == "__main__":
    ensure_bucket(MINIO_BUCKET)
    create_sample_data()
    process_csv_to_minio()

    # Verificar se podemos ler de volta os dados
    print("\nLendo dados do MinIO:")
    con.execute(f"""
    SELECT * FROM read_parquet('s3://{MINIO_BUCKET}/pessoas.parquet', 
        S3_ENDPOINT='{MINIO_ENDPOINT}',
        S3_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}',
        S3_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}',
        S3_URL_STYLE='path'
    )
    """)
    print(con.fetchall())