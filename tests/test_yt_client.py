from unittest.mock import MagicMock, patch
import pytest
from src.extract.youtube_client import YouTubeExtractionError, YouTubeExtractor


@pytest.fixture
def mock_youtube_extractor():
    with patch("src.extract.youtube_client.build") as mock_build:
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        extractor = YouTubeExtractor(api_key="fake-api-key")
        yield extractor, mock_service


def test_get_uploads_playlist_id_success(mock_youtube_extractor):
    extractor, mock_service = mock_youtube_extractor

    mock_service.channels().list().execute.return_value = {
        "items": [
            {"contentDetails": {"relatedPlaylists": {"uploads": "UU123456789"}}}
        ]
    }

    uploads_id = extractor.get_uploads_playlist_id("UC123456789")
    assert uploads_id == "UU123456789"


def test_get_uploads_playlist_id_not_found(mock_youtube_extractor):
    extractor, mock_service = mock_youtube_extractor
    mock_service.channels().list().execute.return_value = {"items": []}

    with pytest.raises(YouTubeExtractionError):
        extractor.get_uploads_playlist_id("UC_NON_EXISTENT")


def test_fetch_video_batch_details_normalization(mock_youtube_extractor):
    extractor, mock_service = mock_youtube_extractor

    mock_service.videos().list().execute.return_value = {
        "items": [
            {
                "id": "vid_abc123",
                "snippet": {
                    "channelId": "chan_001",
                    "channelTitle": "Google Cloud Tech",
                    "title": "Data Pipelines at Scale",
                    "description": "Tech deep dive.",
                    "publishedAt": "2026-09-01T10:00:00Z",
                },
                "statistics": {
                    "viewCount": "1500",
                    "likeCount": "120",
                    "commentCount": "15",
                },
                "topicDetails": {"topicCategories": ["https://en.wikipedia.org/wiki/Technology"]},
            }
        ]
    }

    records = extractor.fetch_video_batch_details(
        video_ids=["vid_abc123"], snapshot_date="2026-09-06"
    )

    assert len(records) == 1
    assert records[0]["video_id"] == "vid_abc123"
    assert records[0]["view_count"] == 1500
    assert records[0]["like_count"] == 120
    assert records[0]["snapshot_date"] == "2026-09-06"
    assert "https://en.wikipedia.org/wiki/Technology" in records[0]["topic_categories"]