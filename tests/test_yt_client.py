"""Unit tests for YouTube extractor using mocked Google API responses."""

from unittest.mock import MagicMock, patch
import pytest

from src.extract.youtube_client import YouTubeExtractor, YouTubeExtractionError


@pytest.fixture
def mock_extractor():
    with patch("src.extract.youtube_client.build") as mock_build:
        extractor = YouTubeExtractor(api_key="test_key")
        yield extractor, extractor.service


def test_get_uploads_playlist_id_success(mock_extractor):
    extractor, mock_service = mock_extractor
    mock_request = MagicMock()
    mock_request.execute.return_value = {
        "items": [{"contentDetails": {"relatedPlaylists": {"uploads": "UU_12345"}}}]
    }
    mock_service.channels().list.return_value = mock_request

    playlist_id = extractor.get_uploads_playlist_id("UC_test")
    assert playlist_id == "UU_12345"


def test_get_uploads_playlist_id_not_found(mock_extractor):
    extractor, mock_service = mock_extractor
    mock_request = MagicMock()
    mock_request.execute.return_value = {"items": []}
    mock_service.channels().list.return_value = mock_request

    with pytest.raises(YouTubeExtractionError):
        extractor.get_uploads_playlist_id("UC_nonexistent")


def test_paginate_playlist_video_ids(mock_extractor):
    extractor, mock_service = mock_extractor
    req1 = MagicMock()
    req1.execute.return_value = {
        "items": [{"contentDetails": {"videoId": "v1"}}],
        "nextPageToken": "token2",
    }
    req2 = MagicMock()
    req2.execute.return_value = {
        "items": [{"contentDetails": {"videoId": "v2"}}],
    }
    mock_service.playlistItems().list.side_effect = [req1, req2]

    ids = list(extractor.paginate_playlist_video_ids("UU_12345"))
    assert ids == ["v1", "v2"]


def test_fetch_video_batch_details(mock_extractor):
    extractor, mock_service = mock_extractor
    mock_request = MagicMock()
    mock_request.execute.return_value = {
        "items": [
            {
                "id": "vid_1",
                "snippet": {
                    "channelId": "ch_1",
                    "channelTitle": "Tech Channel",
                    "title": "Pipeline Architecture",
                    "description": "Building ELT",
                    "publishedAt": "2026-09-01T12:00:00Z",
                },
                "statistics": {
                    "viewCount": "100",
                    "likeCount": "10",
                    "commentCount": "2",
                },
                "topicDetails": {"topicCategories": []},
            }
        ]
    }
    mock_service.videos().list.return_value = mock_request

    records = extractor.fetch_video_batch_details(["vid_1"], snapshot_date="2026-09-06")
    assert len(records) == 1
    assert records[0]["video_id"] == "vid_1"
    assert records[0]["view_count"] == 100
    assert "extracted_at" in records[0]
