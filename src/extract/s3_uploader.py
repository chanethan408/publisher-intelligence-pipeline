import gzip
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import boto3
from botocore.exceptions import ClientError
from src.utils.logger import get_logger

logger = get_logger(__name__)


class S3UploadError(Exception):
    """Raised when writing to S3 fails."""


class S3RawStorage:
    """Manages raw, immutable data deposits into AWS S3."""

    def __init__(self, bucket_name: str, client: Optional[Any] = None):
        self.bucket_name = bucket_name
        self.s3_client = client or boto3.client("s3")

    @staticmethod
    def generate_s3_key(execution_date: str, timestamp_epoch: int) -> str:
        """Constructs partitioned target path."""
        return (
            f"raw/entity=videos/date={execution_date}/"
            f"payload_{timestamp_epoch}.json.gz"
        )

    def upload_raw_payload(
        self,
        payload_records: List[Dict[str, Any]],
        execution_date: str,
    ) -> Dict[str, Any]:
        """Compresses payload in-memory and writes object directly to S3."""
        if not payload_records:
            logger.warning("Zero records provided for upload. Aborting S3 write.")
            return {"uploaded": False, "records": 0, "s3_uri": None}

        timestamp_epoch = int(datetime.now(timezone.utc).timestamp())
        s3_key = self.generate_s3_key(execution_date, timestamp_epoch)
        s3_uri = f"s3://{self.bucket_name}/{s3_key}"

        serialized_json = json.dumps(payload_records, ensure_ascii=False)
        compressed_bytes = gzip.compress(serialized_json.encode("utf-8"))

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=compressed_bytes,
                ContentType="application/json",
                ContentEncoding="gzip",
            )

            metadata = {
                "uploaded": True,
                "records_count": len(payload_records),
                "bytes_uploaded": len(compressed_bytes),
                "s3_uri": s3_uri,
                "s3_key": s3_key,
                "execution_date": execution_date,
            }
            logger.info(
                "Raw payload uploaded successfully to S3",
                extra={"extra_payload": metadata},
            )
            return metadata
        except ClientError as exc:
            logger.error(
                "AWS ClientError encountered during S3 put_object",
                extra={"extra_payload": {"error": str(exc), "target": s3_uri}},
            )
            raise S3UploadError(f"Failed to upload data to {s3_uri}") from exc