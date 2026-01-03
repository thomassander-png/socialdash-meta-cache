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
                
                # Fetch and cache metrics (pass media_type for proper metric selection)
                metrics = cache_instagram_media_metrics(client, cur, media_id, media.get('media_type'))
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


def cache_instagram_media_metrics(client: MetaClient, cur, media_id: str, media_type: str = None) -> bool:
    """
    Cache metrics for a single Instagram media item.
    
    Args:
        client: MetaClient instance
        cur: Database cursor
        media_id: Instagram media ID
        media_type: Type of media (IMAGE, VIDEO, CAROUSEL_ALBUM, REEL)
    
    Returns:
        True if metrics were cached successfully
    """
    import json
    
    try:
        insights = {}
        raw_data = {}
        
        # Determine which metrics to fetch based on media type
        # For Reels: reach, plays, likes, comments, shares, saved
        # For other media: reach, impressions, likes, comments, saved
        
        # Fetch basic metrics (likes, comments) - available for all media types
        media_data = client.get(
            f"/{media_id}",
            fields="like_count,comments_count,media_type"
        )
        
        likes = media_data.get('like_count', 0) if media_data else 0
        comments = media_data.get('comments_count', 0) if media_data else 0
        actual_media_type = media_data.get('media_type') if media_data else media_type
        raw_data['media'] = media_data
        
        # Fetch insights - different metrics available for different media types
        # Try to get all available metrics
        try:
            # Standard insights for images and carousels
            insights_data = client.get(
                f"/{media_id}/insights",
                metric="impressions,reach,saved,shares,profile_visits"
            )
            
            if insights_data and 'data' in insights_data:
                for item in insights_data['data']:
                    name = item.get('name')
                    values = item.get('values', [{}])
                    if values:
                        insights[name] = values[0].get('value')
                raw_data['insights'] = insights_data
        except Exception as e:
            logger.debug(f"Could not fetch standard insights for {media_id}: {e}")
        
        # For Reels and Videos, try to get plays/video_views
        plays = None
        if actual_media_type in ('REEL', 'VIDEO', 'REELS'):
            try:
                # Try 'plays' metric first (for Reels)
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
                    raw_data['plays'] = plays_data
            except Exception as e:
                logger.debug(f"Could not fetch plays for {media_id}: {e}")
                
                # Try 'video_views' as fallback
                try:
                    video_data = client.get(
                        f"/{media_id}/insights",
                        metric="video_views"
                    )
                    if video_data and 'data' in video_data:
                        for item in video_data['data']:
                            if item.get('name') == 'video_views':
                                values = item.get('values', [{}])
                                if values:
                                    plays = values[0].get('value')
                        raw_data['video_views'] = video_data
                except Exception as e2:
                    logger.debug(f"Could not fetch video_views for {media_id}: {e2}")
        
        # Insert metrics snapshot
        cur.execute("""
            INSERT INTO ig_post_metrics (
                media_id, snapshot_time, likes, comments, saves, reach, 
                impressions, plays, shares, profile_visits, raw_json
            ) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            media_id,
            likes,
            comments,
            insights.get('saved'),
            insights.get('reach'),
            insights.get('impressions'),
            plays,
            insights.get('shares'),
            insights.get('profile_visits'),
            json.dumps(raw_data) if raw_data else None
        ))
        
        logger.debug(f"Cached metrics for {media_id}: likes={likes}, comments={comments}, "
                    f"reach={insights.get('reach')}, saves={insights.get('saved')}, plays={plays}")
        
        return True
        
    except Exception as e:
        logger.warning(f"Error caching metrics for media {media_id}: {e}")
        return False


def discover_ig_accounts_from_fb_pages(client: MetaClient, fb_page_ids: List[str]) -> List[str]:
    """
    Discover Instagram Business Accounts linked to Facebook Pages.
    
    Args:
        client: MetaClient instance
        fb_page_ids: List of Facebook Page IDs
    
    Returns:
        List of discovered Instagram account IDs
    """
    ig_account_ids = []
    conn = get_connection()
    if not conn:
        logger.error("No database connection for IG account discovery")
        return ig_account_ids
    
    cur = conn.cursor()
    
    for page_id in fb_page_ids:
        try:
            # Get Instagram Business Account linked to this page
            response = client.get(
                f"/{page_id}",
                fields="instagram_business_account{id,username,name,profile_picture_url,followers_count,media_count}"
            )
            
            if not response:
                continue
            
            ig_data = response.get('instagram_business_account')
            if not ig_data:
                logger.debug(f"No IG account linked to FB Page {page_id}")
                continue
            
            ig_id = ig_data.get('id')
            ig_username = ig_data.get('username', '')
            ig_name = ig_data.get('name', ig_username)
            
            if not ig_id:
                continue
            
            # Upsert to ig_accounts table
            cur.execute("""
                INSERT INTO ig_accounts (account_id, username, name, followers_count, media_count, linked_fb_page_id, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (account_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    name = COALESCE(EXCLUDED.name, ig_accounts.name),
                    followers_count = EXCLUDED.followers_count,
                    media_count = EXCLUDED.media_count,
                    linked_fb_page_id = EXCLUDED.linked_fb_page_id,
                    updated_at = NOW()
            """, (
                ig_id,
                ig_username,
                ig_name,
                ig_data.get('followers_count'),
                ig_data.get('media_count'),
                page_id
            ))
            
            ig_account_ids.append(ig_id)
            logger.info(f"Discovered IG Account: @{ig_username} ({ig_id}) linked to FB Page {page_id}")
            
        except Exception as e:
            logger.warning(f"Could not get IG account for page {page_id}: {e}")
            continue
    
    conn.commit()
    cur.close()
    conn.close()
    
    return ig_account_ids


def run_instagram_cache(
    days_back: int = 45,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Run the Instagram caching process.
    
    If IG_ACCOUNT_IDS is not configured, automatically discovers Instagram Business Accounts
    linked to the configured Facebook Pages.
    
    Args:
        days_back: Number of days to look back
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
    
    Returns:
        Summary of caching results
    """
    config = Config()
    client = MetaClient()
    
    # Get Instagram account IDs from config
    ig_account_ids = config.get_ig_account_ids()
    
    # If no IG accounts configured, try to discover from FB pages
    if not ig_account_ids:
        logger.info("No IG_ACCOUNT_IDS configured, attempting auto-discovery from FB pages...")
        fb_page_ids = config.get_fb_page_ids()
        
        if fb_page_ids:
            ig_account_ids = discover_ig_accounts_from_fb_pages(client, fb_page_ids)
            logger.info(f"Auto-discovered {len(ig_account_ids)} Instagram accounts from {len(fb_page_ids)} FB pages")
        
        if not ig_account_ids:
            logger.warning("No Instagram accounts found (neither configured nor discovered)")
            return {"error": "No Instagram accounts found", "accounts_cached": 0, "posts_cached": 0, "metrics_cached": 0}
    
    logger.info(f"Starting Instagram cache for {len(ig_account_ids)} accounts")
    
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
