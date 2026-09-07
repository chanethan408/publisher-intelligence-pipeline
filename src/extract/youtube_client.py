from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


class YouTubeExtractionError(Exception):
    """Raised when an unrecoverable error occurs interacting with the YouTube API."""


class YouTubeExtractor:
    """Extracts publisher video telemetry via playlist traversal."""

    def __init__(self, api_key: str):
        self.service: Resource = build(
            "youtube", "v3", developerKey=api_key, cache_discovery=False
        )

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((HttpError, TimeoutError)),
    )
    def get_uploads_playlist_id(self, channel_id: str) -> str:
        """Derives the channel's uploads playlist ID. Cost: 1 unit."""
        try:
            response = (
                self.service.channels()
                .list(part="contentDetails", id=channel_id)
                .execute()
            )
            items = response.get("items", [])
            if not items:
                raise YouTubeExtractionError(f"No channel found for ID: {channel_id}")

            return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
        except HttpError as exc:
            logger.error(
                f"Failed to retrieve uploads playlist for channel: {channel_id}",
                extra={"extra_payload": {"error": str(exc)}},
            )
            raise

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((HttpError, TimeoutError)),
    )
    def _fetch_playlist_page(
        self, playlist_id: str, page_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetches a single page of items from a playlist. Cost: 1 unit."""
        return (
            self.service.playlistItems()
            .list(
                part="contentDetails,snippet",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=page_token,
            )
            .execute()
        )

    def paginate_playlist_video_ids(
        self,
        playlist_id: str,
        published_after: Optional[datetime] = None,
    ) -> Generator[str, None, None]:
        """Traverses the uploads playlist and yields video IDs."""
        page_token: Optional[str] = None

        while True:
            payload = self._fetch_playlist_page(playlist_id, page_token)
            items = payload.get("items", [])
            if not items:
                break

            for item in items:
                raw_published = item["contentDetails"].get("videoPublishedAt")
                if raw_published and published_after:
                    published_dt = datetime.fromisoformat(
                        raw_published.replace("Z", "+00:00")
                    )
                    if published_dt < published_after:
                        return

                yield item["contentDetails"]["videoId"]

            page_token = payload.get("nextPageToken")
            if not page_token:
                break

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((HttpError, TimeoutError)),
    )
    def fetch_video_batch_details(
        self, video_ids: List[str], snapshot_date: str
    ) -> List[Dict[str, Any]]:
        """Hydrates up to 50 video IDs with metadata and metrics. Cost: 1 unit."""
        if not video_ids:
            return []

        if len(video_ids) > 50:
            raise ValueError("YouTube API limits videos.list to batches <= 50")

        response = (
            self.service.videos()
            .list(
                part="snippet,statistics,topicDetails",
                id=",".join(video_ids),
                maxResults=50,
            )
            .execute()
        )

        extracted_records: List[Dict[str, Any]] = []
        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            topic_details = item.get("topicDetails", {})

            record = {
                "video_id": item["id"],
                "channel_id": snippet.get("channelId"),
                "channel_title": snippet.get("channelTitle"),
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "published_at": snippet.get("publishedAt"),
                "view_count": int(stats.get("viewCount", 0)),
                "like_count": int(stats.get("likeCount", 0)),
                "comment_count": int(stats.get("commentCount", 0)),
                "topic_categories": topic_details.get("topicCategories", []),
                "snapshot_date": snapshot_date,
            }
            extracted_records.append(record)

        return extracted_records