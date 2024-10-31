import os
import logging
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from sc4py.env import env, env_as_list, env_as_bool


logger = logging.getLogger(__name__)


# Configuração das credenciais do bucket de origem e destino
SOURCE_BUCKET_NAME = env("SOURCE_BUCKET_NAME")
SOURCE_ACCESS_KEY = env("SOURCE_ACCESS_KEY")
SOURCE_SECRET_KEY = env("SOURCE_SECRET_KEY")
DESTINATION_BUCKET_NAME = env("DESTINATION_BUCKET_NAME")
DESTINATION_ACCESS_KEY = env("DESTINATION_ACCESS_KEY")
DESTINATION_SECRET_KEY = env("DESTINATION_SECRET_KEY")

# Validar credenciais
if not all([SOURCE_BUCKET_NAME, SOURCE_ACCESS_KEY, SOURCE_SECRET_KEY,
            DESTINATION_BUCKET_NAME, DESTINATION_ACCESS_KEY, DESTINATION_SECRET_KEY]):
    raise ValueError("Credenciais ou nomes de buckets estão faltando no arquivo .env")

def migrate_objects():
    try:

        # Conectar aos buckets S3 com credenciais específicas
        source_s3 = boto3.client("s3",
            aws_access_key_id=SOURCE_ACCESS_KEY,
            aws_secret_access_key=SOURCE_SECRET_KEY
        )

        destination_s3 = boto3.client(
            "s3",
            aws_access_key_id=DESTINATION_ACCESS_KEY,
            aws_secret_access_key=DESTINATION_SECRET_KEY
        )

        # Listar objetos no bucket de origem
        response = source_s3.list_objects_v2(Bucket=SOURCE_BUCKET_NAME)

        if "Contents" not in response:
            logger.error("Nenhum objeto encontrado no bucket de origem.")
            return

        for item in response["Contents"]:
            file_key = item["Key"]
            logger.info(f"Migrando '{file_key}'...")

            # Copiar o objeto para o bucket de destino
            copy_source = {"Bucket": SOURCE_BUCKET_NAME,"Key": file_key}

            destination_s3.copy(copy_source, DESTINATION_BUCKET_NAME, file_key)
            logger.info(f"'{file_key}' migrado com sucesso.")

        logger.info("Migração concluída.")

    except NoCredentialsError:
        logger.error("Erro: Credenciais não encontradas.")
    except PartialCredentialsError:
        logger.error("Erro: Credenciais incompletas.")
    except Exception as e:
        logger.error(f"Erro durante a migração: {e}")


if __name__ == "__main__":
    # Executar a migração
    logging.basicConfig(level=logging.INFO)
    migrate_objects()
