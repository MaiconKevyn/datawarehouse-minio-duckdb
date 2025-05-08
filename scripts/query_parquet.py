#!/usr/bin/env python3
import os
import duckdb
import pandas as pd
import boto3
from botocore.client import Config
from dotenv import load_dotenv

# ============================================================================= #
# Descrição: Script para análise exploratória de dados de saúde (SIH)
# armazenados como arquivos Parquet no MinIO usando DuckDB como engine
# analítica em memória.
#
# Arquitetura:
# 1. Conexão com MinIO (S3-compatible) para inventário dos arquivos
# 2. Configuração de DuckDB para leitura federada diretamente do MinIO
# 3. Execução de análises SQL federadas contra os dados brutos
#
# Benefícios desta abordagem:
# - Evita transferência desnecessária de dados (data locality)
# - Aproveita a compressão e codificação colunar do Parquet
# - Utiliza o poder de processamento do DuckDB para análise in-memory
# - Mantém os dados no data lake sem duplicação
# ============================================================================= #

# Carrega variáveis do ambiente (.env) - padrão 12-factor app
load_dotenv()

# Configurações do MinIO - Abstração da origem dos dados
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
S3_PREFIX = os.getenv("MINIO_S3_PREFIX")


def main():
    # Inicializa cliente S3 para comunicação com MinIO
    # Nota: A API S3 é usada como interface padrão para object storage
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),  # Necessário para compatibilidade
        region_name='us-east-1'
    )

    print(f"🔍 Listando arquivos Parquet em s3://{MINIO_BUCKET}/{S3_PREFIX}")

    # Inventário de arquivos Parquet no bucket usando paginação
    # Importante: A paginação garante escalabilidade para buckets com muitos objetos
    parquet_files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=S3_PREFIX):
        contents = page.get('Contents', [])
        for obj in contents:
            if obj['Key'].endswith('.parquet'):
                parquet_files.append(obj['Key'])
                print(f"- {obj['Key']}")

    if not parquet_files:
        print("Nenhum arquivo Parquet encontrado!")
        return

    # Inicialização do DuckDB em memória (in-memory analytical database)
    # Funciona como um data warehouse efêmero para análises rápidas
    con = duckdb.connect(database=':memory:')

    # Configuração do conector S3 do DuckDB para acesso externo
    # Esta configuração permite query federation (consultas diretas nos arquivos remotos)
    con.execute(f"""
        SET s3_region='us-east-1';
        SET s3_endpoint='{MINIO_ENDPOINT.replace('http://', '')}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_url_style='path';
        SET s3_use_ssl=false;
    """)

    # Preparação dos caminhos S3 para consulta federada
    # Utilizando path completo para acesso direto aos arquivos Parquet
    s3_paths = [f"s3://{MINIO_BUCKET}/{path}" for path in parquet_files]
    parquet_paths_str = ", ".join(f"'{path}'" for path in s3_paths)

    print("\n📊 Executando consulta nos dados...")

    # Análise 1: Estatísticas globais dos dados
    # Métricas fundamentais para entender o volume e cardinalidade dos dados
    query = f"""
        SELECT 
            COUNT(*) as total_registros,
            COUNT(DISTINCT N_AIH) as total_AIHs
        FROM read_parquet([{parquet_paths_str}])
    """

    try:
        # Execução da consulta e recuperação como DataFrame para manipulação
        result = con.execute(query).fetchdf()

        # Apresentação dos resultados iniciais
        print("\n✅ Resultados da consulta:")
        print(result)

        # Análise 2: Distribuição de frequência de procedimentos
        # Análise exploratória para identificar procedimentos mais comuns
        print("\n📈 Top 5 procedimentos mais frequentes:")
        detailed_query = f"""
            SELECT 
                PROC_REA, 
                COUNT(*) as quantidade
            FROM read_parquet([{parquet_paths_str}])
            GROUP BY PROC_REA
            ORDER BY quantidade DESC
            LIMIT 5
        """
        detailed_result = con.execute(detailed_query).fetchdf()
        print(detailed_result)

        # NOTA: Em um ambiente de produção, poderíamos expandir com:
        # - Persistência dos resultados agregados em tabelas materializadas
        # - Geração de visualizações e dashboards automatizados
        # - Análises estatísticas mais avançadas (outliers, tendências, etc.)
        # - Modelos preditivos sobre os dados históricos

    except Exception as e:
        print(f"\n❌ Erro ao executar a consulta: {e}")
        # Em produção: implementar logging estruturado e sistema de alertas


if __name__ == "__main__":
    main()