import os
import logging
from typing import Any
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from sc4py.env import env, env_as_list, env_as_bool


logger = logging.getLogger(__name__)

class S3Setting:
    def __init__(self, kind: str):
        # Configuração das credenciais do bucket de origem
        self.BUCKET_NAME = env(f"{kind}_BUCKET_NAME")
        self.ACCESS_KEY = env(f"{kind}_ACCESS_KEY")
        self.SECRET_KEY = env(f"{kind}_SECRET_KEY")

        # Validar credenciais
        if not all([self.BUCKET_NAME, self.ACCESS_KEY, self.SECRET_KEY]):
            raise ValueError(f"Bucket name or credentials for {kind} not informed in environment variables.")


class S3Sync:
    def __init__(self):
        self.SOURCE = S3Setting("SOURCE")
        self.DESTINATION = S3Setting("DESTINATION")

        self.sources = {}
        self.destinations = {}
        self.client = None
        self.to_sync = None

    def __get_all_s3(self, setting: S3Setting) -> dict[str, Any]:
        logger.info(f"Reading bulk {setting.BUCKET_NAME}")
        aws_session = boto3.Session(aws_access_key_id=setting.ACCESS_KEY, aws_secret_access_key=setting.SECRET_KEY)
        s3_resource = aws_session.resource("s3")
        result = {x.key: x for x in s3_resource.Bucket(setting.BUCKET_NAME).objects.all()}
        logger.info(f"Bulk {setting.BUCKET_NAME} readed")
        return result

    def read(self):
        self.sources = self.__get_all_s3(self.SOURCE)
        self.destinations = self.__get_all_s3(self.DESTINATION)
        self.to_sync = [v for k, v in self.sources.items() if k not in self.destinations]

        self.client = boto3.client(
            "s3",
            aws_access_key_id=self.DESTINATION.ACCESS_KEY,
            aws_secret_access_key=self.DESTINATION.SECRET_KEY
        )

    def execute(self):
        self.read()

        try:
            total = len(self.sources)
            done = total - len(self.to_sync)
            todo = total - done
            actual = 0.0
            for key in self.sources.keys():
                actual += 1.0
                per = actual / todo * 100
                try:
                    logger.info(f"Mitrating '{key}'... {per:.3f}%, {actual}/{todo}, {done} discarded. Total: {total}.")
                    self.client.copy({"Bucket": self.SOURCE.BUCKET_NAME, "Key": key}, self.DESTINATION.BUCKET_NAME, key)
                    logger.info(f"'{key}' migrated with success.")
                except ClientError as e:
                    logger.error(f"Error migrating: {e}. We will continue.")
            logger.info("Migration done.")
        except Exception as e:
            logger.error(f"Error ({type(e)}): {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    S3Sync().execute()
    # migrate_objects()
