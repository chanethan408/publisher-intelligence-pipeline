"""Snowflake loader executing idempotent COPY INTO from S3 raw partitions."""

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
from typing import Any, Dict, Optional
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.errors import DatabaseError

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger

load_dotenv()
logger = get_logger("stage_snowflake")


class SnowflakeStageLoader:
    """Manages raw telemetry loading into Snowflake RAW_INGEST schema."""

    def __init__(self, connection_params: Optional[Dict[str, Any]] = None):
        """Initializes Snowflake client using environment parameters."""
        self.conn_params = connection_params or {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "PUBLISHER_DWH"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW_INGEST"),
        }

    def load_partition(self, execution_date: str) -> Dict[str, Any]:
        """Loads S3 files matching execution_date partition via COPY INTO.

        Args:
            execution_date: Date string formatted YYYY-MM-DD.

        Returns:
            Dict[str, Any]: Execution summary metrics.
        """
        pattern_sql = f"""
        COPY INTO PUBLISHER_DWH.RAW_INGEST.RAW_VIDEOS (
            raw_payload,
            execution_date,
            ingested_at,
            source_file
        )
        FROM (
            SELECT 
                $1 AS raw_payload,
                TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'date=([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1, 1, 'e', 1)) AS execution_date,
                CURRENT_TIMESTAMP() AS ingested_at,
                METADATA$FILENAME AS source_file
            FROM @PUBLISHER_DWH.RAW_INGEST.STAGE_S3_RAW
        )
        PATTERN = '.*date={execution_date}/.*\\.json\\.gz'
        FILE_FORMAT = (FORMAT_NAME = 'PUBLISHER_DWH.RAW_INGEST.FF_JSON_GZIP')
        ON_ERROR = 'ABORT_STATEMENT';
        """

        logger.info(
            f"Executing COPY INTO for execution_date={execution_date}",
            extra={"extra_payload": {"execution_date": execution_date}},
        )

        try:
            with snowflake.connector.connect(**self.conn_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(pattern_sql)
                    results = cur.fetchall()

                    rows_loaded = sum(
                        row[1]
                        for row in results
                        if len(row) > 1 and isinstance(row[1], int)
                    )
                    files_loaded = len(results)

                    summary = {
                        "execution_date": execution_date,
                        "files_loaded": files_loaded,
                        "rows_loaded": rows_loaded,
                        "status": "SUCCESS",
                    }
                    logger.info("Snowflake staging complete", extra={"extra_payload": summary})
                    return summary
        except DatabaseError as err:
            logger.error(
                "Snowflake COPY INTO failed",
                extra={"extra_payload": {"execution_date": execution_date, "error": str(err)}},
            )
            raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Snowflake Raw Stage Loader")
    parser.add_argument(
        "--execution-date",
        type=str,
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Logical execution date in YYYY-MM-DD format",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    loader = SnowflakeStageLoader()
    loader.load_partition(execution_date=args.execution_date)