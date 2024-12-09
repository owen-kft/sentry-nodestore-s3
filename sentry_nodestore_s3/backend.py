from __future__ import annotations

from typing import Any, Mapping
from datetime import datetime, timedelta
import boto3
from botocore.config import Config
from psycopg2 import connect
from psycopg2.extras import RealDictCursor
from sentry.utils.codecs import Codec, ZstdCodec
from sentry.nodestore.base import NodeStorage
from sentry.nodestore.django import DjangoNodeStorage


class S3PassthroughDjangoNodeStorage(DjangoNodeStorage, NodeStorage):
    compression_strategies: Mapping[str, Codec[bytes, bytes]] = {
        "zstd": ZstdCodec(),
    }

    def __init__(
        self,
        delete_through=False,
        write_through=False,
        read_through=False,
        compression=True,
        bucket_name=None,
        region_name=None,
        bucket_path=None,
        endpoint_url=None,
        retry_attempts=3,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        db_host="10.0.10.222",
        db_port=5432,
        db_name="postres",
        db_user="postgres",
        db_password="",
    ):
        self.delete_through = delete_through
        self.write_through = write_through
        self.read_through = read_through

        if compression:
            self.compression = "zstd"
        else:
            self.compression = None

        self.bucket_name = bucket_name
        self.bucket_path = bucket_path
        self.client = boto3.client(
            config=Config(
                retries={
                    'mode': 'standard',
                    'max_attempts': retry_attempts,
                }
            ),
            region_name=region_name,
            service_name='s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        self.pg_connection = connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
        )
        self.pg_connection.autocommit = True

    def insert_id_and_timestamp(self, id: str):
        """Insert id and current timestamp into the database."""
        timestamp = datetime.utcnow()
        with self.pg_connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO nodestore_node (id, timestamp) 
                VALUES (%s, %s) 
                ON CONFLICT (id) DO NOTHING;
                """,
                (id, timestamp),
            )

    def delete_id(self, id: str):
        """Delete id from the database."""
        with self.pg_connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM nodestore_node WHERE id = %s;",
                (id,),
            )

    def fetch_timestamp(self, id: str) -> datetime | None:
        """Fetch timestamp for the given id."""
        with self.pg_connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT timestamp FROM nodestore_node WHERE id = %s;",
                (id,),
            )
            result = cursor.fetchone()
            return result["timestamp"] if result else None

    def __construct_s3_key(self, id: str, timestamp: datetime) -> str:
        """Construct S3 key using timestamp and id."""
        date_prefix = timestamp.strftime("%Y/%m/%d")
        return f"{self.bucket_path}/{date_prefix}/{id}" if self.bucket_path else f"{date_prefix}/{id}"

    def __write_to_bucket(self, id: str, data: Any) -> None:
        """Insert id and timestamp into the database and write to S3."""
        self.insert_id_and_timestamp(id)

        content_encoding = ''
        if self.compression is not None:
            codec = self.compression_strategies[self.compression]
            compressed_data = codec.encode(data)

            # Check if compression is worth it
            if len(compressed_data) <= len(data):
                data = compressed_data
                content_encoding = self.compression

        key = self.__construct_s3_key(id, datetime.utcnow())
        self.client.put_object(
            Key=key,
            Body=data,
            Bucket=self.bucket_name,
            ContentEncoding=content_encoding,
        )

    def __delete_from_bucket(self, id: str) -> None:
        """Delete id from the database and S3."""
        self.delete_id(id)

        timestamp = self.fetch_timestamp(id)
        if not timestamp:
            raise ValueError(f"No timestamp found for ID {id}")

        key = self.__construct_s3_key(id, timestamp)
        self.client.delete_object(
            Key=key,
            Bucket=self.bucket_name,
        )

    def __read_from_bucket(self, id: str) -> bytes | None:
        """Fetch timestamp, construct S3 key, and read from S3."""
        timestamp = self.fetch_timestamp(id)
        if not timestamp:
            raise ValueError(f"No timestamp found for ID {id}")

        key = self.__construct_s3_key(id, timestamp)
        try:
            obj = self.client.get_object(
                Key=key,
                Bucket=self.bucket_name,
            )

            data = obj.get('Body').read()
            codec = self.compression_strategies.get(obj.get('ContentEncoding'))
            return codec.decode(data) if codec else data
        except self.client.exceptions.NoSuchKey:
            return None

    # Override existing methods to include PostgreSQL logic
    def delete(self, id):
        if self.delete_through:
            super().delete(id)
        self.__delete_from_bucket(id)
        self._delete_cache_item(id)

    def _get_bytes(self, id: str) -> bytes | None:
        if self.read_through:
            return self.__read_from_bucket(id) or super()._get_bytes(id)
        return self.__read_from_bucket(id)

    def _set_bytes(self, id: str, data: Any, ttl: timedelta | None = None) -> None:
        if self.write_through:
            super()._set_bytes(id, data, ttl)
        self.__write_to_bucket(id, data)

    def delete_multi(self, id_list: list[str]) -> None:
        if self.delete_through:
            super().delete_multi(id_list)
        for id in id_list:
            self.__delete_from_bucket(id)
        self._delete_cache_items(id_list)

    def cleanup(self, cutoff_timestamp: datetime) -> None:
        if self.delete_through:
            super().cleanup(cutoff_timestamp)
