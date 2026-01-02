"""
Module for caching Facebook posts from the Meta Graph API.
Includes media URL extraction for thumbnail caching.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from .config import Config
from .db import Database
from .meta_client import MetaClient, MetaAPIError

logger = logging.getLogger(__name__)


class ThumbnailFetcher:
    """Handles fetching thumbnails from various sources."""
    
    def __init__(self, client: MetaClient):
        self.client = client
        self.stats = {
            "graph_api": 0,
            "open_graph": 0,
            "none": 0
        }
    
    def get_post_media_urls(self, post_id: str, permalink: Optional[str]) -> Dict[str, Optional[str]]:
        """
        Get media URLs for a post from various sources.
        
        Priority:
        1. Graph API attachments
        2. OpenGraph meta tags from permalink
        3. None
        
        Returns:
            Dict with media_url, thumbnail_url, og_image_url, preview_source
        """
        result = {
            "media_url": None,
            "thumbnail_url": None,
            "og_image_url": None,
            "preview_source": "none"
        }
        
        # Try Graph API first
        try:
            attachments = self.client.get(
                f"/{post_id}",
                fields="attachments{media{image{src}},subattachments{media{image{src}}}}"
            )
            
            if attachments and "attachments" in attachments:
                att_data = attachments["attachments"].get("data", [])
                if att_data:
                    first_att = att_data[0]
                    
                    # Try main media
                    media = first_att.get("media", {})
                    image = media.get("image", {})
                    if image.get("src"):
                        result["media_url"] = image["src"]
                        result["thumbnail_url"] = image["src"]
                        result["preview_source"] = "graph_api"
                        self.stats["graph_api"] += 1
                        return result
                    
                    # Try subattachments (for albums)
                    subatts = first_att.get("subattachments", {}).get("data", [])
                    if subatts:
                        first_sub = subatts[0]
                        sub_media = first_sub.get("media", {})
                        sub_image = sub_media.get("image", {})
                        if sub_image.get("src"):
                            result["media_url"] = sub_image["src"]
                            result["thumbnail_url"] = sub_image["src"]
                            result["preview_source"] = "graph_api"
                            self.stats["graph_api"] += 1
                            return result
        except Exception as e:
            logger.debug(f"Could not get attachments for {post_id}: {e}")
        
        # Fallback to OpenGraph
        if permalink:
            og_image = self._get_og_image(permalink)
            if og_image:
                result["og_image_url"] = og_image
                result["thumbnail_url"] = og_image
                result["preview_source"] = "open_graph"
                self.stats["open_graph"] += 1
                return result
        
        self.stats["none"] += 1
        return result
    
    def _get_og_image(self, url: str) -> Optional[str]:
        """Extract og:image from a URL."""
        try:
            response = requests.get(
                url,
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0 (compatible; SocialDash/1.0)"},
                allow_redirects=True
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            og_image = soup.find('meta', property='og:image')
            if og_image and og_image.get('content'):
                return og_image['content']
        except Exception as e:
            logger.debug(f"Could not get OG image from {url}: {e}")
        
        return None


class PostCacher:
    """Handles caching of Facebook posts."""
    
    def __init__(self, config: Config, db: Database, client: MetaClient):
        self.config = config
        self.db = db
        self.client = client
        self.thumbnail_fetcher = ThumbnailFetcher(client)
    
    def cache_page_posts(
        self,
        page_id: str,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        fetch_thumbnails: bool = True
    ) -> Dict[str, int]:
        """
        Cache posts from a single page.
        
        Returns:
            Dict with counts: {"fetched": N, "upserted": N}
        """
        # Default to lookback_days if no date range specified
        if since is None:
            since = datetime.utcnow() - timedelta(days=self.config.lookback_days)
        if until is None:
            until = datetime.utcnow()
        
        logger.info(f"Caching posts for page {page_id} from {since} to {until}")
        
        # First, ensure page exists in database
        try:
            page_info = self.client.get_page_info(page_id)
            self.db.upsert_page(
                page_id=page_id,
                name=page_info.get("name", f"Page {page_id}")
            )
        except MetaAPIError as e:
            logger.warning(f"Could not fetch page info for {page_id}: {e}")
            # Still try to upsert with basic info
            self.db.upsert_page(page_id=page_id, name=f"Page {page_id}")
        
        # Fetch posts from API
        try:
            posts = self.client.get_page_posts(
                page_id=page_id,
                since=since,
                until=until,
                lookback_days=self.config.lookback_days
            )
        except MetaAPIError as e:
            logger.error(f"Failed to fetch posts for page {page_id}: {e}")
            return {"fetched": 0, "upserted": 0}
        
        if not posts:
            logger.info(f"No posts found for page {page_id}")
            return {"fetched": 0, "upserted": 0}
        
        # Transform posts for database
        db_posts = []
        for post in posts:
            created_time_str = post.get("created_time")
            if created_time_str:
                created_time = datetime.fromisoformat(
                    created_time_str.replace("Z", "+00:00")
                )
            else:
                continue
            
            post_data = {
                "post_id": post["id"],
                "page_id": page_id,
                "created_time": created_time,
                "type": post.get("type"),
                "permalink": post.get("permalink_url"),
                "message": post.get("message", "")[:5000] if post.get("message") else None,
            }
            
            # Fetch thumbnails if enabled
            if fetch_thumbnails:
                media_urls = self.thumbnail_fetcher.get_post_media_urls(
                    post["id"],
                    post.get("permalink_url")
                )
                post_data.update(media_urls)
            
            db_posts.append(post_data)
        
        # Batch upsert to database
        upserted = self.db.upsert_posts_batch(db_posts)
        
        logger.info(f"Cached {upserted} posts for page {page_id}")
        logger.info(f"Thumbnail stats: {self.thumbnail_fetcher.stats}")
        
        return {
            "fetched": len(posts),
            "upserted": upserted,
            "posts": db_posts,  # Return for metrics caching
            "thumbnail_stats": self.thumbnail_fetcher.stats
        }
    
    def cache_all_pages(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        fetch_thumbnails: bool = True
    ) -> Dict[str, Dict[str, int]]:
        """
        Cache posts from all configured pages.
        
        Returns:
            Dict mapping page_id to result counts
        """
        results = {}
        total_fetched = 0
        total_upserted = 0
        
        for page_id in self.config.fb_page_ids:
            logger.info(f"Processing page: {page_id}")
            
            try:
                result = self.cache_page_posts(
                    page_id=page_id,
                    since=since,
                    until=until,
                    fetch_thumbnails=fetch_thumbnails
                )
                results[page_id] = result
                total_fetched += result["fetched"]
                total_upserted += result["upserted"]
                
            except Exception as e:
                logger.error(f"Error caching page {page_id}: {e}")
                results[page_id] = {"fetched": 0, "upserted": 0, "error": str(e)}
        
        logger.info(f"Total: Fetched {total_fetched} posts, upserted {total_upserted}")
        
        return results


def run_cache_posts(
    config: Config,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    fetch_thumbnails: bool = True
) -> Dict[str, Dict[str, int]]:
    """
    Main entry point for caching posts.
    """
    db = Database(config)
    client = MetaClient(config)
    cacher = PostCacher(config, db, client)
    
    return cacher.cache_all_pages(since=since, until=until, fetch_thumbnails=fetch_thumbnails)
