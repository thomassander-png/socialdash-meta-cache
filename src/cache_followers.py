"""
Module for caching follower counts from Facebook and Instagram.
Stores daily snapshots for growth tracking.
"""

import logging
from datetime import datetime, date
from typing import Dict, List, Optional

from .config import Config
from .db import Database
from .meta_client import MetaClient, MetaAPIError

logger = logging.getLogger(__name__)


class FollowerCacher:
    """Handles caching of follower counts for Facebook and Instagram."""
    
    def __init__(self, config: Config, db: Database, client: MetaClient):
        self.config = config
        self.db = db
        self.client = client
    
    def cache_fb_page_followers(self, page_id: str) -> Optional[int]:
        """
        Cache follower count for a single Facebook page.
        
        Args:
            page_id: The Facebook page ID
            
        Returns:
            Follower count or None if failed
        """
        try:
            # Get page info with fan_count (followers)
            page_info = self.client.get(
                f"/{page_id}",
                fields="id,name,fan_count,followers_count"
            )
            
            if not page_info:
                logger.warning(f"No page info returned for {page_id}")
                return None
            
            # fan_count is the primary follower metric for Pages
            followers = page_info.get("fan_count") or page_info.get("followers_count")
            
            if followers is None:
                logger.warning(f"No follower count available for page {page_id}")
                return None
            
            # Store snapshot
            today = date.today()
            self._store_fb_follower_snapshot(page_id, today, followers)
            
            logger.info(f"Cached FB followers for {page_id}: {followers:,}")
            return followers
            
        except MetaAPIError as e:
            logger.error(f"Failed to get followers for FB page {page_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error caching FB followers for {page_id}: {e}")
            return None
    
    def cache_ig_account_followers(self, account_id: str) -> Optional[int]:
        """
        Cache follower count for a single Instagram account.
        
        Args:
            account_id: The Instagram account ID
            
        Returns:
            Follower count or None if failed
        """
        try:
            # Get account info with followers_count
            account_info = self.client.get(
                f"/{account_id}",
                fields="id,username,followers_count"
            )
            
            if not account_info:
                logger.warning(f"No account info returned for {account_id}")
                return None
            
            followers = account_info.get("followers_count")
            
            if followers is None:
                logger.warning(f"No follower count available for IG account {account_id}")
                return None
            
            # Store snapshot
            today = date.today()
            self._store_ig_follower_snapshot(account_id, today, followers)
            
            logger.info(f"Cached IG followers for {account_id}: {followers:,}")
            return followers
            
        except MetaAPIError as e:
            logger.error(f"Failed to get followers for IG account {account_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error caching IG followers for {account_id}: {e}")
            return None
    
    def _store_fb_follower_snapshot(
        self, 
        page_id: str, 
        snapshot_date: date, 
        followers_count: int
    ) -> bool:
        """Store a Facebook follower snapshot in the database."""
        try:
            conn = self.db.get_connection()
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO fb_follower_history (page_id, snapshot_date, followers_count)
                VALUES (%s, %s, %s)
                ON CONFLICT (page_id, snapshot_date) 
                DO UPDATE SET followers_count = EXCLUDED.followers_count
            """, (page_id, snapshot_date, followers_count))
            
            conn.commit()
            cur.close()
            return True
            
        except Exception as e:
            logger.error(f"Failed to store FB follower snapshot: {e}")
            return False
    
    def _store_ig_follower_snapshot(
        self, 
        account_id: str, 
        snapshot_date: date, 
        followers_count: int
    ) -> bool:
        """Store an Instagram follower snapshot in the database."""
        try:
            conn = self.db.get_connection()
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO ig_follower_history (account_id, snapshot_date, followers_count)
                VALUES (%s, %s, %s)
                ON CONFLICT (account_id, snapshot_date) 
                DO UPDATE SET followers_count = EXCLUDED.followers_count
            """, (account_id, snapshot_date, followers_count))
            
            conn.commit()
            cur.close()
            return True
            
        except Exception as e:
            logger.error(f"Failed to store IG follower snapshot: {e}")
            return False
    
    def cache_all_fb_followers(self) -> Dict[str, int]:
        """
        Cache follower counts for all configured Facebook pages.
        
        Returns:
            Dict mapping page_id to follower count
        """
        results = {}
        success = 0
        failed = 0
        
        for page_id in self.config.fb_page_ids:
            followers = self.cache_fb_page_followers(page_id)
            if followers is not None:
                results[page_id] = followers
                success += 1
            else:
                failed += 1
        
        logger.info(f"FB Followers cached: {success} success, {failed} failed")
        return results
    
    def cache_all_ig_followers(self) -> Dict[str, int]:
        """
        Cache follower counts for all configured Instagram accounts.
        
        Returns:
            Dict mapping account_id to follower count
        """
        results = {}
        success = 0
        failed = 0
        
        for account_id in self.config.ig_account_ids:
            followers = self.cache_ig_account_followers(account_id)
            if followers is not None:
                results[account_id] = followers
                success += 1
            else:
                failed += 1
        
        logger.info(f"IG Followers cached: {success} success, {failed} failed")
        return results
    
    def get_fb_follower_growth(
        self, 
        page_id: str, 
        month: date
    ) -> Dict[str, int]:
        """
        Get follower growth for a Facebook page for a specific month.
        
        Args:
            page_id: The Facebook page ID
            month: The month to get growth for (first day of month)
            
        Returns:
            Dict with current_followers, previous_followers, growth, growth_percentage
        """
        try:
            conn = self.db.get_connection()
            cur = conn.cursor()
            
            # Get current month's latest follower count
            cur.execute("""
                SELECT followers_count FROM fb_follower_history
                WHERE page_id = %s
                AND snapshot_date >= DATE_TRUNC('month', %s::date)
                AND snapshot_date < DATE_TRUNC('month', %s::date) + INTERVAL '1 month'
                ORDER BY snapshot_date DESC
                LIMIT 1
            """, (page_id, month, month))
            
            current_row = cur.fetchone()
            current = current_row[0] if current_row else 0
            
            # Get previous month's latest follower count
            cur.execute("""
                SELECT followers_count FROM fb_follower_history
                WHERE page_id = %s
                AND snapshot_date >= DATE_TRUNC('month', %s::date) - INTERVAL '1 month'
                AND snapshot_date < DATE_TRUNC('month', %s::date)
                ORDER BY snapshot_date DESC
                LIMIT 1
            """, (page_id, month, month))
            
            previous_row = cur.fetchone()
            previous = previous_row[0] if previous_row else 0
            
            cur.close()
            
            growth = current - previous
            growth_pct = round((growth / previous * 100), 2) if previous > 0 else 0
            
            return {
                "current_followers": current,
                "previous_followers": previous,
                "growth": growth,
                "growth_percentage": growth_pct
            }
            
        except Exception as e:
            logger.error(f"Failed to get FB follower growth: {e}")
            return {
                "current_followers": 0,
                "previous_followers": 0,
                "growth": 0,
                "growth_percentage": 0
            }
    
    def get_ig_follower_growth(
        self, 
        account_id: str, 
        month: date
    ) -> Dict[str, int]:
        """
        Get follower growth for an Instagram account for a specific month.
        
        Args:
            account_id: The Instagram account ID
            month: The month to get growth for (first day of month)
            
        Returns:
            Dict with current_followers, previous_followers, growth, growth_percentage
        """
        try:
            conn = self.db.get_connection()
            cur = conn.cursor()
            
            # Get current month's latest follower count
            cur.execute("""
                SELECT followers_count FROM ig_follower_history
                WHERE account_id = %s
                AND snapshot_date >= DATE_TRUNC('month', %s::date)
                AND snapshot_date < DATE_TRUNC('month', %s::date) + INTERVAL '1 month'
                ORDER BY snapshot_date DESC
                LIMIT 1
            """, (account_id, month, month))
            
            current_row = cur.fetchone()
            current = current_row[0] if current_row else 0
            
            # Get previous month's latest follower count
            cur.execute("""
                SELECT followers_count FROM ig_follower_history
                WHERE account_id = %s
                AND snapshot_date >= DATE_TRUNC('month', %s::date) - INTERVAL '1 month'
                AND snapshot_date < DATE_TRUNC('month', %s::date)
                ORDER BY snapshot_date DESC
                LIMIT 1
            """, (account_id, month, month))
            
            previous_row = cur.fetchone()
            previous = previous_row[0] if previous_row else 0
            
            cur.close()
            
            growth = current - previous
            growth_pct = round((growth / previous * 100), 2) if previous > 0 else 0
            
            return {
                "current_followers": current,
                "previous_followers": previous,
                "growth": growth,
                "growth_percentage": growth_pct
            }
            
        except Exception as e:
            logger.error(f"Failed to get IG follower growth: {e}")
            return {
                "current_followers": 0,
                "previous_followers": 0,
                "growth": 0,
                "growth_percentage": 0
            }


def run_cache_followers(config: Config) -> Dict[str, Dict[str, int]]:
    """
    Main entry point for caching follower counts.
    
    Returns:
        Dict with 'facebook' and 'instagram' results
    """
    db = Database(config)
    client = MetaClient(config)
    cacher = FollowerCacher(config, db, client)
    
    logger.info("Starting follower count caching...")
    
    results = {
        "facebook": cacher.cache_all_fb_followers(),
        "instagram": cacher.cache_all_ig_followers()
    }
    
    logger.info(f"Follower caching complete: {len(results['facebook'])} FB, {len(results['instagram'])} IG")
    
    return results
