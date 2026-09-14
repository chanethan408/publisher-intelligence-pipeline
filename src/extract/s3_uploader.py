"""AWS S3 raw storage client for immutable compressed JSON telemetry."""

from datetime import datetime, timezone
import gzip
import io
import json
import time
from typing import Any, Dict, List, Optional
import boto3
from botocore.exceptions import ClientError

from src.utils.logger import get_logger

logger = get_logger(__name__)


class S3UploadError(Exception):
    """Raised when an S3 upload fails or violates immutability."""


class S3RawStorage:
    """Manages immutable raw JSON.GZ uploads to S3 partitions."""

    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
        client: Optional[Any] = None,
    ):
        self.bucket_name = bucket_name

        if client is not None:
            self.s3_client = client
        else:
            client_kwargs: Dict[str, Any] = {"region_name": region_name}
            if aws_access_key_id and aws_secret_access_key:
                client_kwargs["aws_access_key_id"] = aws_access_key_id
                client_kwargs["aws_secret_access_key"] = aws_secret_access_key
            self.s3_client = boto3.client("s3", **client_kwargs)

    @staticmethod
    def generate_s3_key(
        execution_date: str, timestamp: Optional[int] = None, entity: str = "videos"
    ) -> str:
        """Constructs an immutable Hive-partitioned S3 key."""
        ts = timestamp if timestamp is not None else int(time.time())
        return f"raw/entity={entity}/date={execution_date}/payload_{ts}.json.gz"

    def upload_raw_payload(
        self,
        records: List[Dict[str, Any]],
        execution_date: str,
        entity: str = "videos",
    ) -> Dict[str, Any]:
        """Compresses records to JSON.GZ and uploads them to S3 immutably."""
        if not records:
            logger.warning(
                "Payload is empty; skipping upload",
                extra={"extra_payload": {"execution_date": execution_date}},
            )
            return {"uploaded": False, "records": 0, "s3_uri": ""}

        s3_key = self.generate_s3_key(execution_date=execution_date, entity=entity)

        # Invariant check: enforce immutability
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            raise S3UploadError(
                f"Object s3://{self.bucket_name}/{s3_key} already exists. Overwrites prohibited."
            )
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            # 404/NoSuchKey confirms the key is new and safe to write
            if error_code not in ("404", "NoSuchKey"):
                logger.error(
                    "S3 head_object pre-check failed",
                    extra={"extra_payload": {"error": str(exc)}},
                )
                raise

        # Compress to in-memory buffer
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
            json_bytes = json.dumps(records, default=str).encode("utf-8")
            gz.write(json_bytes)

        buffer.seek(0)
        compressed_bytes = buffer.getvalue()

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=compressed_bytes,
                ContentType="application/json",
                ContentEncoding="gzip",
                Metadata={
                    "records_count": str(len(records)),
                    "execution_date": execution_date,
                    "uploaded_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(
                "S3 payload upload successful",
                extra={
                    "extra_payload": {
                        "s3_uri": s3_uri,
                        "records_count": len(records),
                        "bytes_uploaded": len(compressed_bytes),
                    }
                },
            )
            return {
                "uploaded": True,
                "records_count": len(records),
                "bytes_uploaded": len(compressed_bytes),
                "s3_uri": s3_uri,
            }
        except Exception as exc:
            logger.error(
                "Failed to put object to S3",
                extra={"extra_payload": {"s3_key": s3_key, "error": str(exc)}},
            )
            raise S3UploadError(str(exc)) from exc
