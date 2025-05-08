import io
import boto3
from botocore.client import Config
import os
import pandas as pd
from dotenv import load_dotenv


def get_minio_client():
    """
    Cria e retorna um cliente MinIO/S3 configurado com variáveis de ambiente.
    """
    load_dotenv()

    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]
    region = os.getenv("AWS_REGION", "us-east-1")

    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name=region
    )
    return s3

def criar_bucket(nome_bucket):
    """
    Cria um bucket no MinIO se ele não existir.

    Args:
        nome_bucket (str): Nome do bucket a ser criado

    Returns:
        bool: True se o bucket foi criado ou já existia, False em caso de erro
    """
    s3 = get_minio_client()

    try:
        # Verificar se o bucket já existe
        s3.head_bucket(Bucket=nome_bucket)
        print(f"Bucket '{nome_bucket}' já existe.")
        return True
    except:
        try:
            # Criar o bucket
            print(f"Bucket '{nome_bucket}' não existe. Criando...")
            s3.create_bucket(Bucket=nome_bucket)
            print(f"Bucket '{nome_bucket}' criado com sucesso.")
            return True
        except Exception as e:
            print(f"Erro ao criar bucket '{nome_bucket}': {e}")
            return False


def upload_csv(dataframe, nome_bucket, nome_arquivo):
    """
    Converte um DataFrame para CSV e faz upload para um bucket no MinIO.

    Args:
        dataframe (pandas.DataFrame): DataFrame a ser convertido e enviado
        nome_bucket (str): Nome do bucket de destino
        nome_arquivo (str): Nome do arquivo CSV no bucket

    Returns:
        bool: True se o upload foi bem-sucedido, False caso contrário
    """
    s3 = get_minio_client()

    try:
        # Empacotar em buffer de memória
        buf = io.BytesIO()
        dataframe.to_csv(buf, index=False)
        buf.seek(0)

        # Fazer upload para o MinIO
        print(f"Enviando {nome_arquivo} para s3://{nome_bucket}/{nome_arquivo}")
        s3.upload_fileobj(buf, nome_bucket, nome_arquivo)
        print("Upload concluído com sucesso!")
        return True
    except Exception as e:
        print(f"Erro ao fazer upload: {e}")
        return False