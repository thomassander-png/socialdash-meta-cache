"""
Module for caching Facebook posts from the Meta Graph API.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from .config import Config
from .db import Database
from .meta_client import MetaClient, MetaAPIError

logger = logging.getLogger(__name__)


class PostCacher:
    """Handles caching of Facebook posts."""
    
    def __init__(self, config: Config, db: Database, client: MetaClient):
        self.config = config
        self.db = db
        self.client = client
    
    def cache_page_posts(
        self,
        page_id: str,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
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
            
            db_posts.append({
                "post_id": post["id"],
                "page_id": page_id,
                "created_time": created_time,
                "type": post.get("type"),
                "permalink": post.get("permalink_url"),
                "message": post.get("message", "")[:5000] if post.get("message") else None,
            })
        
        # Batch upsert to database
        upserted = self.db.upsert_posts_batch(db_posts)
        
        logger.info(f"Cached {upserted} posts for page {page_id}")
        
        return {
            "fetched": len(posts),
            "upserted": upserted,
            "posts": db_posts  # Return for metrics caching
        }
    
    def cache_all_pages(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
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
                    until=until
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
    until: Optional[datetime] = None
) -> Dict[str, Dict[str, int]]:
    """
    Main entry point for caching posts.
    """
    db = Database(config)
    client = MetaClient(config)
    cacher = PostCacher(config, db, client)
    
    return cacher.cache_all_pages(since=since, until=until)
