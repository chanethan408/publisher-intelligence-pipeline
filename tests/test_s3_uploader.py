from unittest.mock import MagicMock
import gzip
import json
import pytest
from src.extract.s3_uploader import S3RawStorage, S3UploadError


def test_generate_s3_key_format():
    key = S3RawStorage.generate_s3_key("2026-09-06", 1725638400)
    assert key == "raw/entity=videos/date=2026-09-06/payload_1725638400.json.gz"


def test_upload_raw_payload_success():
    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = Exception("404")  # Simulate file doesn't exist
    from botocore.exceptions import ClientError
    mock_s3.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "head_object")

    storage = S3RawStorage(bucket_name="test-bucket", client=mock_s3)
    sample_records = [{"video_id": "v1", "views": 100}]

    result = storage.upload_raw_payload(sample_records, execution_date="2026-09-06")

    assert result["uploaded"] is True
    assert result["records_count"] == 1
    assert "s3://test-bucket/raw/entity=videos/date=2026-09-06/" in result["s3_uri"]

    mock_s3.put_object.assert_called_once()
    call_args = mock_s3.put_object.call_args[1]
    assert call_args["Bucket"] == "test-bucket"
    assert call_args["ContentEncoding"] == "gzip"

    decompressed = gzip.decompress(call_args["Body"]).decode("utf-8")
    assert json.loads(decompressed) == sample_records


def test_upload_empty_payload_skips():
    mock_s3 = MagicMock()
    storage = S3RawStorage(bucket_name="test-bucket", client=mock_s3)
    result = storage.upload_raw_payload([], execution_date="2026-09-06")

    assert result["uploaded"] is False
    assert result["records"] == 0
    mock_s3.put_object.assert_not_called()


def test_upload_existing_file_raises_immutability_error():
    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {"ContentLength": 100}  # Simulates file exists

    storage = S3RawStorage(bucket_name="test-bucket", client=mock_s3)
    sample_records = [{"video_id": "v1"}]

    with pytest.raises(S3UploadError, match="already exists. Overwrites prohibited."):
        storage.upload_raw_payload(sample_records, execution_date="2026-09-06")