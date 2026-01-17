"""
Instagram Account Insights caching module.
Fetches account-level insights from the Instagram Graph API.

These metrics are at the account level, not per-post:
- profile_links_taps (Klicks auf Profil-Buttons)
- total_interactions (Gesamte Interaktionen)
- views (Aufrufe/Impressions)
- reach (Reichweite)
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from .config import Config
from .meta_client import MetaClient
from .db import get_connection

logger = logging.getLogger(__name__)


def cache_ig_account_insights(
    client: MetaClient,
    account_ids: List[str],
    days_back: int = 30
) -> Dict[str, Any]:
    """
    Cache Instagram account-level insights.
    
    Args:
        client: MetaClient instance
        account_ids: List of Instagram account IDs
        days_back: Number of days to look back (max 30 for most metrics)
    
    Returns:
        Dictionary with caching results
    """
    conn = get_connection()
    if not conn:
        logger.error("No database connection")
        return {"accounts_processed": 0, "insights_cached": 0}
    
    cur = conn.cursor()
    accounts_processed = 0
    insights_cached = 0
    
    # Calculate date range (Instagram insights are limited to 30 days)
    until = datetime.now()
    since = until - timedelta(days=min(days_back, 30))
    
    # Convert to Unix timestamps
    since_ts = int(since.timestamp())
    until_ts = int(until.timestamp())
    
    for account_id in account_ids:
        try:
            logger.info(f"Fetching account insights for {account_id}")
            
            insights = {}
            
            # Fetch interaction metrics
            # Available metrics: accounts_engaged, comments, likes, profile_links_taps, 
            # reach, replies, saved, shares, total_interactions, views
            
            interaction_metrics = [
                "profile_links_taps",
                "total_interactions", 
                "views",
                "reach",
                "accounts_engaged"
            ]
            
            for metric in interaction_metrics:
                try:
                    response = client.get(
                        f"/{account_id}/insights",
                        metric=metric,
                        period="day",
                        metric_type="total_value",
                        since=since_ts,
                        until=until_ts
                    )
                    
                    if response and 'data' in response:
                        for item in response['data']:
                            if item.get('name') == metric:
                                total_value = item.get('total_value', {})
                                value = total_value.get('value', 0)
                                insights[metric] = value
                                
                                # Check for breakdowns (e.g., contact_button_type for profile_links_taps)
                                breakdowns = total_value.get('breakdowns', [])
                                if breakdowns:
                                    insights[f"{metric}_breakdown"] = breakdowns
                                    
                                logger.debug(f"Got {metric}={value} for account {account_id}")
                                
                except Exception as e:
                    logger.debug(f"Could not fetch {metric} for {account_id}: {e}")
            
            # Fetch profile_links_taps with breakdown by contact_button_type
            try:
                response = client.get(
                    f"/{account_id}/insights",
                    metric="profile_links_taps",
                    period="day",
                    metric_type="total_value",
                    breakdown="contact_button_type",
                    since=since_ts,
                    until=until_ts
                )
                
                if response and 'data' in response:
                    for item in response['data']:
                        if item.get('name') == 'profile_links_taps':
                            total_value = item.get('total_value', {})
                            breakdowns = total_value.get('breakdowns', [])
                            
                            # Parse breakdown by contact type
                            clicks_by_type = {}
                            for breakdown in breakdowns:
                                results = breakdown.get('results', [])
                                for result in results:
                                    dim_values = result.get('dimension_values', [])
                                    if dim_values:
                                        contact_type = dim_values[0]
                                        clicks_by_type[contact_type] = result.get('value', 0)
                            
                            insights['profile_links_taps_by_type'] = clicks_by_type
                            
                            # Calculate total clicks
                            total_clicks = sum(clicks_by_type.values())
                            insights['profile_links_taps'] = total_clicks
                            
                            # Extract specific click types
                            insights['email_clicks'] = clicks_by_type.get('EMAIL', 0)
                            insights['call_clicks'] = clicks_by_type.get('CALL', 0)
                            insights['direction_clicks'] = clicks_by_type.get('DIRECTION', 0)
                            insights['text_clicks'] = clicks_by_type.get('TEXT', 0)
                            
            except Exception as e:
                logger.debug(f"Could not fetch profile_links_taps breakdown for {account_id}: {e}")
            
            # Insert insights snapshot
            if insights:
                cur.execute("""
                    INSERT INTO ig_account_insights (
                        account_id, snapshot_date, 
                        profile_links_taps, email_clicks, call_clicks, direction_clicks, text_clicks,
                        total_interactions, views, reach, accounts_engaged,
                        raw_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (account_id, snapshot_date) DO UPDATE SET
                        profile_links_taps = EXCLUDED.profile_links_taps,
                        email_clicks = EXCLUDED.email_clicks,
                        call_clicks = EXCLUDED.call_clicks,
                        direction_clicks = EXCLUDED.direction_clicks,
                        text_clicks = EXCLUDED.text_clicks,
                        total_interactions = EXCLUDED.total_interactions,
                        views = EXCLUDED.views,
                        reach = EXCLUDED.reach,
                        accounts_engaged = EXCLUDED.accounts_engaged,
                        raw_json = EXCLUDED.raw_json,
                        updated_at = NOW()
                """, (
                    account_id,
                    datetime.now().date(),
                    insights.get('profile_links_taps'),
                    insights.get('email_clicks'),
                    insights.get('call_clicks'),
                    insights.get('direction_clicks'),
                    insights.get('text_clicks'),
                    insights.get('total_interactions'),
                    insights.get('views'),
                    insights.get('reach'),
                    insights.get('accounts_engaged'),
                    json.dumps(insights)
                ))
                
                insights_cached += 1
                logger.info(f"Cached insights for {account_id}: clicks={insights.get('profile_links_taps')}, "
                           f"interactions={insights.get('total_interactions')}, views={insights.get('views')}")
            
            accounts_processed += 1
            
        except Exception as e:
            logger.error(f"Error caching insights for account {account_id}: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    
    return {
        "accounts_processed": accounts_processed,
        "insights_cached": insights_cached
    }


def run_ig_account_insights_cache(days_back: int = 30) -> Dict[str, Any]:
    """
    Run the Instagram account insights caching process.
    
    Args:
        days_back: Number of days to look back
    
    Returns:
        Summary of caching results
    """
    from .cache_instagram import discover_ig_accounts_from_fb_pages
    
    config = Config()
    client = MetaClient(config)
    
    # Get Instagram account IDs
    ig_account_ids = config.get_ig_account_ids()
    
    # If no IG accounts configured, try to discover from FB pages
    if not ig_account_ids:
        logger.info("No IG_ACCOUNT_IDS configured, attempting auto-discovery from FB pages...")
        fb_page_ids = config.get_fb_page_ids()
        
        if fb_page_ids:
            ig_account_ids = discover_ig_accounts_from_fb_pages(client, fb_page_ids)
    
    if not ig_account_ids:
        logger.warning("No Instagram accounts found")
        return {"error": "No Instagram accounts found", "accounts_processed": 0, "insights_cached": 0}
    
    logger.info(f"Caching account insights for {len(ig_account_ids)} Instagram accounts")
    
    return cache_ig_account_insights(client, ig_account_ids, days_back)
