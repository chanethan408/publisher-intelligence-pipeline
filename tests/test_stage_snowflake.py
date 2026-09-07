"""Unit tests for Snowflake staging loader using mocked Snowflake connector."""

from unittest.mock import MagicMock, patch
import pytest

from src.load.stage_snowflake import SnowflakeStageLoader


@patch("src.load.stage_snowflake.snowflake.connector.connect")
def test_load_partition_success(mock_connect):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("raw/entity=videos/date=2026-09-06/payload.json.gz", 6931, "LOADED", None)
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value.__enter__.return_value = mock_conn

    fake_creds = {
        "account": "test_acc",
        "user": "test_user",
        "password": "test_password",
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "database": "PUBLISHER_DWH",
        "schema": "RAW_INGEST",
    }

    loader = SnowflakeStageLoader(connection_params=fake_creds)
    result = loader.load_partition("2026-09-06")

    assert result["status"] == "SUCCESS"
    assert result["files_loaded"] == 1
    assert result["rows_loaded"] == 6931
    mock_cursor.execute.assert_called_once()
    assert "date=2026-09-06" in mock_cursor.execute.call_args[0][0]