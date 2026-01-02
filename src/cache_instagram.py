"""
Instagram data caching module.
Fetches Instagram media and metrics from the Graph API and stores them in the database.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from .config import Config
from .meta_client import MetaClient
from .db import get_connection

logger = logging.getLogger(__name__)


def cache_instagram_accounts(client: MetaClient, account_ids: List[str]) -> int:
    """
    Cache Instagram account information.
    
    Args:
        client: MetaClient instance
        account_ids: List of Instagram account IDs
    
    Returns:
        Number of accounts cached
    """
    conn = get_connection()
    if not conn:
        logger.error("No database connection")
        return 0
    
    cached_count = 0
    cur = conn.cursor()
    
    for account_id in account_ids:
        try:
            # Fetch account info
            account_data = client.get(
                f"/{account_id}",
                fields="id,username,name,followers_count,media_count"
            )
            
            if not account_data:
                logger.warning(f"No data for account {account_id}")
                continue
            
            # Upsert account
            cur.execute("""
                INSERT INTO ig_accounts (account_id, username, name, followers_count, media_count, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (account_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    name = EXCLUDED.name,
                    followers_count = EXCLUDED.followers_count,
                    media_count = EXCLUDED.media_count,
                    updated_at = NOW()
            """, (
                account_data.get('id'),
                account_data.get('username'),
                account_data.get('name'),
                account_data.get('followers_count'),
                account_data.get('media_count')
            ))
            
            cached_count += 1
            logger.info(f"Cached account: {account_data.get('username', account_id)}")
            
        except Exception as e:
            logger.error(f"Error caching account {account_id}: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    
    return cached_count


def cache_instagram_media(
    client: MetaClient,
    account_ids: List[str],
    days_back: int = 45,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> Dict[str, int]:
    """
    Cache Instagram media (posts, reels) for the specified accounts.
    
    Args:
        client: MetaClient instance
        account_ids: List of Instagram account IDs
        days_back: Number of days to look back (default 45)
        start_date: Optional start date for filtering
        end_date: Optional end date for filtering
    
    Returns:
        Dictionary with counts: posts_cached, metrics_cached
    """
    conn = get_connection()
    if not conn:
        logger.error("No database connection")
        return {"posts_cached": 0, "metrics_cached": 0}
    
    # Calculate date range
    if start_date and end_date:
        since = start_date
        until = end_date
    else:
        until = datetime.now()
        since = until - timedelta(days=days_back)
    
    logger.info(f"Caching Instagram media from {since.date()} to {until.date()}")
    
    posts_cached = 0
    metrics_cached = 0
    cur = conn.cursor()
    
    for account_id in account_ids:
        try:
            # Fetch media
            media_list = client.get_paginated(
                f"/{account_id}/media",
                fields="id,caption,media_type,timestamp,permalink,media_url,thumbnail_url"
            )
            
            logger.info(f"Fetched {len(media_list)} media items for account {account_id}")
            
            for media in media_list:
                # Parse timestamp
                timestamp_str = media.get('timestamp', '')
                if timestamp_str:
                    try:
                        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    except:
                        timestamp = datetime.now()
                else:
                    timestamp = datetime.now()
                
                # Filter by date range
                if timestamp.replace(tzinfo=None) < since or timestamp.replace(tzinfo=None) > until:
                    continue
                
                media_id = media.get('id')
                if not media_id:
                    continue
                
                # Upsert media
                cur.execute("""
                    INSERT INTO ig_posts (media_id, account_id, media_type, caption, permalink, timestamp, media_url, thumbnail_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (media_id) DO UPDATE SET
                        caption = EXCLUDED.caption,
                        permalink = EXCLUDED.permalink,
                        media_url = EXCLUDED.media_url,
                        thumbnail_url = EXCLUDED.thumbnail_url
                """, (
                    media_id,
                    account_id,
                    media.get('media_type'),
                    media.get('caption'),
                    media.get('permalink'),
                    timestamp,
                    media.get('media_url'),
                    media.get('thumbnail_url')
                ))
                
                posts_cached += 1
                
                # Fetch and cache metrics
                metrics = cache_instagram_media_metrics(client, cur, media_id)
                if metrics:
                    metrics_cached += 1
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error caching media for account {account_id}: {e}")
            conn.rollback()
    
    cur.close()
    conn.close()
    
    logger.info(f"Cached {posts_cached} posts, {metrics_cached} metric snapshots")
    return {"posts_cached": posts_cached, "metrics_cached": metrics_cached}


def cache_instagram_media_metrics(client: MetaClient, cur, media_id: str) -> bool:
    """
    Cache metrics for a single Instagram media item.
    
    Args:
        client: MetaClient instance
        cur: Database cursor
        media_id: Instagram media ID
    
    Returns:
        True if metrics were cached successfully
    """
    try:
        # Fetch insights
        # Note: Not all media types support all insights
        insights_data = client.get(
            f"/{media_id}/insights",
            metric="impressions,reach,saved,shares"
        )
        
        # Parse insights
        insights = {}
        if insights_data and 'data' in insights_data:
            for item in insights_data['data']:
                name = item.get('name')
                values = item.get('values', [{}])
                if values:
                    insights[name] = values[0].get('value', 0)
        
        # Fetch basic metrics (likes, comments)
        media_data = client.get(
            f"/{media_id}",
            fields="like_count,comments_count"
        )
        
        likes = media_data.get('like_count', 0) if media_data else 0
        comments = media_data.get('comments_count', 0) if media_data else 0
        
        # For reels, try to get plays
        plays = None
        try:
            plays_data = client.get(
                f"/{media_id}/insights",
                metric="plays"
            )
            if plays_data and 'data' in plays_data:
                for item in plays_data['data']:
                    if item.get('name') == 'plays':
                        values = item.get('values', [{}])
                        if values:
                            plays = values[0].get('value')
        except:
            pass
        
        # Insert metrics snapshot
        cur.execute("""
            INSERT INTO ig_post_metrics (
                media_id, snapshot_time, likes, comments, saves, reach, impressions, plays, shares, raw_json
            ) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            media_id,
            likes,
            comments,
            insights.get('saved'),
            insights.get('reach'),
            insights.get('impressions'),
            plays,
            insights.get('shares'),
            None  # raw_json - could store full response if needed
        ))
        
        return True
        
    except Exception as e:
        logger.warning(f"Error caching metrics for media {media_id}: {e}")
        return False


def run_instagram_cache(
    days_back: int = 45,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Run the Instagram caching process.
    
    Args:
        days_back: Number of days to look back
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
    
    Returns:
        Summary of caching results
    """
    config = Config()
    
    # Get Instagram account IDs from config
    ig_account_ids = config.get_ig_account_ids()
    if not ig_account_ids:
        logger.warning("No Instagram account IDs configured (IG_ACCOUNT_IDS)")
        return {"error": "No Instagram account IDs configured"}
    
    logger.info(f"Starting Instagram cache for {len(ig_account_ids)} accounts")
    
    client = MetaClient()
    
    # Parse dates if provided
    start_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
    
    # Cache accounts
    accounts_cached = cache_instagram_accounts(client, ig_account_ids)
    
    # Cache media
    media_result = cache_instagram_media(
        client,
        ig_account_ids,
        days_back=days_back,
        start_date=start_dt,
        end_date=end_dt
    )
    
    return {
        "accounts_cached": accounts_cached,
        "posts_cached": media_result.get("posts_cached", 0),
        "metrics_cached": media_result.get("metrics_cached", 0)
    }
