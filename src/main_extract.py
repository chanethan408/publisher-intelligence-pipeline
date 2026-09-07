"""CLI entrypoint for YouTube publisher telemetry ingestion into raw AWS S3 storage."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List

# Ensure project root is in sys.path regardless of execution method
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

from src.extract.s3_uploader import S3RawStorage
from src.extract.youtube_client import YouTubeExtractor
from src.utils.logger import get_logger

load_dotenv()

logger = get_logger("main_extract")

def parse_args() -> argparse.Namespace:
    """Parses command-line execution parameters."""
    parser = argparse.ArgumentParser(
        description="Extract YouTube publisher telemetry and upload to raw S3."
    )
    parser.add_argument(
        "--execution-date",
        type=str,
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Logical execution date in YYYY-MM-DD format (defaults to UTC today).",
    )
    parser.add_argument(
        "--config-path",
        type=str,
        default="config/channels.json",
        help="Path to curated channels JSON file.",
    )
    return parser.parse_args()


def load_channels(config_path: str) -> List[Dict[str, str]]:
    """Loads curated publisher channels list."""
    path = Path(config_path)
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
        return data.get("channels", [])


def run_pipeline(execution_date: str, config_path: str) -> Dict[str, Any]:
    """Executes the extraction and S3 staging workflow."""
    start_time = time.time()
    api_key = os.getenv("YOUTUBE_API_KEY")
    bucket_name = os.getenv("S3_BUCKET_NAME")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    if not api_key:
        raise ValueError("Missing YOUTUBE_API_KEY environment variable.")
    if not bucket_name:
        raise ValueError("Missing S3_BUCKET_NAME environment variable.")

    logger.info(
        f"Starting extraction run for execution_date={execution_date}",
        extra={"extra_payload": {"execution_date": execution_date}},
    )

    channels = load_channels(config_path)
    logger.info(
        f"Loaded {len(channels)} target channels from {config_path}",
        extra={"extra_payload": {"channel_count": len(channels)}},
    )

    extractor = YouTubeExtractor(api_key=api_key)
    storage = S3RawStorage(bucket_name=bucket_name, region_name=aws_region)

    total_records: List[Dict[str, Any]] = []

    for channel in channels:
        channel_id = channel["channel_id"]
        channel_name = channel.get("name", "Unknown")
        try:
            uploads_id = extractor.get_uploads_playlist_id(channel_id)
            video_ids = list(extractor.paginate_playlist_video_ids(uploads_id))

            logger.info(
                f"Discovered {len(video_ids)} video IDs for channel: {channel_name}",
                extra={"extra_payload": {"channel_id": channel_id, "video_count": len(video_ids)}},
            )

            # Hydrate in 50-video chunks
            for i in range(0, len(video_ids), 50):
                chunk = video_ids[i : i + 50]
                records = extractor.fetch_video_batch_details(
                    chunk, snapshot_date=execution_date
                )
                total_records.extend(records)

        except Exception as exc:
            logger.error(
                f"Extraction failed for channel: {channel_name} ({channel_id})",
                extra={"extra_payload": {"channel_id": channel_id, "error": str(exc)}},
            )
            raise

    # Upload aggregated payload to S3
    upload_result = storage.upload_raw_payload(
        records=total_records, execution_date=execution_date, entity="videos"
    )

    duration_ms = int((time.time() - start_time) * 1000)
    summary = {
        "execution_date": execution_date,
        "records_extracted": len(total_records),
        "bytes_uploaded": upload_result.get("bytes_uploaded", 0),
        "s3_uri": upload_result.get("s3_uri", ""),
        "duration_ms": duration_ms,
        "status": "SUCCESS",
    }

    logger.info("Extraction pipeline finished successfully", extra={"extra_payload": summary})
    return summary


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(execution_date=args.execution_date, config_path=args.config_path)