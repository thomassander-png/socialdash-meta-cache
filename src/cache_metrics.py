"""
Module for caching Facebook post metrics from the Meta Graph API.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from .config import Config
from .db import Database
from .meta_client import MetaClient, MetaAPIError

logger = logging.getLogger(__name__)


class MetricsCacher:
    """Handles caching of Facebook post metrics."""
    
    def __init__(self, config: Config, db: Database, client: MetaClient):
        self.config = config
        self.db = db
        self.client = client
    
    def cache_post_metrics(
        self,
        post_id: str,
        post_data: Optional[Dict] = None
    ) -> Optional[Dict]:
        """
        Cache metrics for a single post.
        
        Args:
            post_id: The Facebook post ID
            post_data: Optional post data from API (contains shares info)
        
        Returns:
            Dict with metrics or None if failed
        """
        post_data = post_data or {}
        
        try:
            metrics = self.client.get_post_full_metrics(post_id, post_data)
            
            # Insert snapshot
            self.db.insert_metrics_snapshot(
                post_id=post_id,
                reactions_total=metrics.get("reactions_total", 0),
                comments_total=metrics.get("comments_total", 0),
                shares_total=metrics.get("shares_total"),
                reach=metrics.get("reach"),
                impressions=metrics.get("impressions"),
                video_3s_views=metrics.get("video_3s_views"),
                shares_limited=metrics.get("shares_limited", True),
                raw_json=metrics
            )
            
            return metrics
            
        except MetaAPIError as e:
            logger.warning(f"Failed to cache metrics for post {post_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error caching metrics for post {post_id}: {e}")
            return None
    
    def cache_metrics_for_posts(
        self,
        posts: List[Dict],
        batch_size: int = 50
    ) -> Dict[str, int]:
        """
        Cache metrics for a list of posts.
        
        Args:
            posts: List of post dicts with at least 'post_id' key
            batch_size: Number of posts to process before logging progress
        
        Returns:
            Dict with counts: {"processed": N, "success": N, "failed": N}
        """
        total = len(posts)
        success = 0
        failed = 0
        
        logger.info(f"Caching metrics for {total} posts")
        
        for i, post in enumerate(posts):
            post_id = post.get("post_id") or post.get("id")
            if not post_id:
                logger.warning(f"Post missing ID: {post}")
                failed += 1
                continue
            
            result = self.cache_post_metrics(post_id, post)
            
            if result:
                success += 1
            else:
                failed += 1
            
            # Progress logging
            if (i + 1) % batch_size == 0:
                logger.info(f"Progress: {i + 1}/{total} posts processed")
        
        logger.info(f"Metrics caching complete: {success} success, {failed} failed")
        
        return {
            "processed": total,
            "success": success,
            "failed": failed
        }
    
    def cache_metrics_for_page(
        self,
        page_id: str,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
    ) -> Dict[str, int]:
        """
        Cache metrics for all posts from a page within a date range.
        """
        # Get posts from database
        posts = self.db.get_posts_by_page(
            page_id=page_id,
            since=since,
            until=until
        )
        
        if not posts:
            logger.info(f"No posts found for page {page_id}")
            return {"processed": 0, "success": 0, "failed": 0}
        
        return self.cache_metrics_for_posts(posts)
    
    def cache_metrics_for_all_pages(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
    ) -> Dict[str, Dict[str, int]]:
        """
        Cache metrics for all posts from all configured pages.
        """
        results = {}
        total_success = 0
        total_failed = 0
        
        for page_id in self.config.fb_page_ids:
            logger.info(f"Caching metrics for page: {page_id}")
            
            result = self.cache_metrics_for_page(
                page_id=page_id,
                since=since,
                until=until
            )
            
            results[page_id] = result
            total_success += result["success"]
            total_failed += result["failed"]
        
        logger.info(f"Total metrics: {total_success} success, {total_failed} failed")
        
        return results


def run_cache_metrics(
    config: Config,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None
) -> Dict[str, Dict[str, int]]:
    """
    Main entry point for caching metrics.
    """
    db = Database(config)
    client = MetaClient(config)
    cacher = MetricsCacher(config, db, client)
    
    return cacher.cache_metrics_for_all_pages(since=since, until=until)
