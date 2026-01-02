"""
Database module for SocialDash Meta Cache.
Handles all PostgreSQL operations.
"""

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, date
from typing import Any, Dict, Generator, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from .config import Config

logger = logging.getLogger(__name__)


def get_connection():
    """Get a database connection from environment variable."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL not set")
        return None
    
    try:
        return psycopg2.connect(database_url)
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None


class Database:
    """PostgreSQL database handler."""
    
    def __init__(self, config: Config):
        self.config = config
        self.connection_string = config.database_url
    
    @contextmanager
    def get_connection(self) -> Generator:
        """Context manager for database connections."""
        conn = psycopg2.connect(self.connection_string)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    @contextmanager
    def get_cursor(self, dict_cursor: bool = True) -> Generator:
        """Context manager for database cursors."""
        with self.get_connection() as conn:
            cursor_factory = RealDictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
            finally:
                cursor.close()
    
    def run_migrations(self, migrations_dir: str = "migrations") -> None:
        """Run SQL migration files in order."""
        migration_files = sorted([
            f for f in os.listdir(migrations_dir)
            if f.endswith(".sql")
        ])
        
        with self.get_cursor(dict_cursor=False) as cursor:
            for migration_file in migration_files:
                filepath = os.path.join(migrations_dir, migration_file)
                logger.info(f"Running migration: {migration_file}")
                
                with open(filepath, "r") as f:
                    sql = f.read()
                    cursor.execute(sql)
                
                logger.info(f"Migration {migration_file} completed")
    
    # ==================== Pages ====================
    
    def upsert_page(self, page_id: str, name: str) -> None:
        """Insert or update a Facebook page."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO fb_pages (page_id, name)
                VALUES (%s, %s)
                ON CONFLICT (page_id) DO UPDATE SET name = EXCLUDED.name
            """, (page_id, name))
        
        logger.debug(f"Upserted page: {page_id} ({name})")
    
    def get_pages(self) -> List[Dict[str, Any]]:
        """Get all tracked pages."""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT * FROM fb_pages ORDER BY name")
            return cursor.fetchall()
    
    # ==================== Posts ====================
    
    def upsert_post(
        self,
        post_id: str,
        page_id: str,
        created_time: datetime,
        post_type: Optional[str] = None,
        permalink: Optional[str] = None,
        message: Optional[str] = None,
        media_url: Optional[str] = None,
        thumbnail_url: Optional[str] = None,
        og_image_url: Optional[str] = None,
        preview_source: Optional[str] = None
    ) -> None:
        """Insert or update a Facebook post."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO fb_posts (post_id, page_id, created_time, type, permalink, message,
                                      media_url, thumbnail_url, og_image_url, preview_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (post_id) DO UPDATE SET
                    type = COALESCE(EXCLUDED.type, fb_posts.type),
                    permalink = COALESCE(EXCLUDED.permalink, fb_posts.permalink),
                    message = COALESCE(EXCLUDED.message, fb_posts.message),
                    media_url = COALESCE(EXCLUDED.media_url, fb_posts.media_url),
                    thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, fb_posts.thumbnail_url),
                    og_image_url = COALESCE(EXCLUDED.og_image_url, fb_posts.og_image_url),
                    preview_source = COALESCE(EXCLUDED.preview_source, fb_posts.preview_source)
            """, (post_id, page_id, created_time, post_type, permalink, message,
                  media_url, thumbnail_url, og_image_url, preview_source))
        
        logger.debug(f"Upserted post: {post_id}")
    
    def upsert_posts_batch(self, posts: List[Dict[str, Any]]) -> int:
        """Batch upsert multiple posts with media URL support."""
        if not posts:
            return 0
        
        with self.get_cursor() as cursor:
            values = [
                (
                    p["post_id"],
                    p["page_id"],
                    p["created_time"],
                    p.get("type"),
                    p.get("permalink"),
                    p.get("message"),
                    p.get("media_url"),
                    p.get("thumbnail_url"),
                    p.get("og_image_url"),
                    p.get("preview_source")
                )
                for p in posts
            ]
            
            execute_values(
                cursor,
                """
                INSERT INTO fb_posts (post_id, page_id, created_time, type, permalink, message,
                                      media_url, thumbnail_url, og_image_url, preview_source)
                VALUES %s
                ON CONFLICT (post_id) DO UPDATE SET
                    type = COALESCE(EXCLUDED.type, fb_posts.type),
                    permalink = COALESCE(EXCLUDED.permalink, fb_posts.permalink),
                    message = COALESCE(EXCLUDED.message, fb_posts.message),
                    media_url = COALESCE(EXCLUDED.media_url, fb_posts.media_url),
                    thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, fb_posts.thumbnail_url),
                    og_image_url = COALESCE(EXCLUDED.og_image_url, fb_posts.og_image_url),
                    preview_source = COALESCE(EXCLUDED.preview_source, fb_posts.preview_source)
                """,
                values
            )
        
        logger.info(f"Batch upserted {len(posts)} posts")
        return len(posts)
    
    def get_posts_by_page(
        self,
        page_id: str,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get posts for a page within a date range."""
        with self.get_cursor() as cursor:
            query = "SELECT * FROM fb_posts WHERE page_id = %s"
            params = [page_id]
            
            if since:
                query += " AND created_time >= %s"
                params.append(since)
            
            if until:
                query += " AND created_time <= %s"
                params.append(until)
            
            query += " ORDER BY created_time DESC"
            
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def get_posts_in_range(
        self,
        since: datetime,
        until: datetime,
        page_ids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get all posts within a date range, optionally filtered by pages."""
        with self.get_cursor() as cursor:
            query = "SELECT * FROM fb_posts WHERE created_time >= %s AND created_time <= %s"
            params = [since, until]
            
            if page_ids:
                query += " AND page_id = ANY(%s)"
                params.append(page_ids)
            
            query += " ORDER BY created_time DESC"
            
            cursor.execute(query, params)
            return cursor.fetchall()
    
    # ==================== Metrics ====================
    
    def insert_metrics_snapshot(
        self,
        post_id: str,
        reactions_total: int = 0,
        comments_total: int = 0,
        shares_total: Optional[int] = None,
        reach: Optional[int] = None,
        impressions: Optional[int] = None,
        video_3s_views: Optional[int] = None,
        shares_limited: bool = True,
        raw_json: Optional[Dict[str, Any]] = None
    ) -> int:
        """Insert a new metrics snapshot for a post."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO fb_post_metrics (
                    post_id, reactions_total, comments_total, shares_total,
                    reach, impressions, video_3s_views, shares_limited, raw_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                post_id, reactions_total, comments_total, shares_total,
                reach, impressions, video_3s_views, shares_limited,
                json.dumps(raw_json) if raw_json else None
            ))
            
            result = cursor.fetchone()
            snapshot_id = result["id"] if result else 0
        
        logger.debug(f"Inserted metrics snapshot {snapshot_id} for post {post_id}")
        return snapshot_id
    
    def insert_metrics_batch(self, metrics_list: List[Dict[str, Any]]) -> int:
        """Batch insert multiple metrics snapshots."""
        if not metrics_list:
            return 0
        
        with self.get_cursor() as cursor:
            values = [
                (
                    m["post_id"],
                    m.get("reactions_total", 0),
                    m.get("comments_total", 0),
                    m.get("shares_total"),
                    m.get("reach"),
                    m.get("impressions"),
                    m.get("video_3s_views"),
                    m.get("shares_limited", True),
                    json.dumps(m.get("raw_json")) if m.get("raw_json") else None
                )
                for m in metrics_list
            ]
            
            execute_values(
                cursor,
                """
                INSERT INTO fb_post_metrics (
                    post_id, reactions_total, comments_total, shares_total,
                    reach, impressions, video_3s_views, shares_limited, raw_json
                )
                VALUES %s
                """,
                values
            )
        
        logger.info(f"Batch inserted {len(metrics_list)} metrics snapshots")
        return len(metrics_list)
    
    def get_latest_metrics_for_post(self, post_id: str) -> Optional[Dict[str, Any]]:
        """Get the most recent metrics snapshot for a post."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM fb_post_metrics
                WHERE post_id = %s
                ORDER BY snapshot_time DESC
                LIMIT 1
            """, (post_id,))
            return cursor.fetchone()
    
    # ==================== Monthly Summary ====================
    
    def upsert_monthly_summary(
        self,
        month: date,
        post_id: str,
        page_id: str,
        reach: Optional[int] = None,
        impressions: Optional[int] = None,
        reactions_total: int = 0,
        comments_total: int = 0,
        video_3s_views: Optional[int] = None
    ) -> None:
        """Insert or update a monthly summary for a post."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO fb_monthly_post_summary (
                    month, post_id, page_id, reach, impressions,
                    reactions_total, comments_total, video_3s_views
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (month, post_id) DO UPDATE SET
                    reach = EXCLUDED.reach,
                    impressions = EXCLUDED.impressions,
                    reactions_total = EXCLUDED.reactions_total,
                    comments_total = EXCLUDED.comments_total,
                    video_3s_views = EXCLUDED.video_3s_views,
                    created_at = NOW()
            """, (
                month, post_id, page_id, reach, impressions,
                reactions_total, comments_total, video_3s_views
            ))
        
        logger.debug(f"Upserted monthly summary for post {post_id} ({month})")
    
    def get_monthly_post_metrics(self, month: date) -> List[Dict[str, Any]]:
        """Get the last metrics snapshot for each post in a given month."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM view_fb_monthly_post_metrics
                WHERE month = %s
                ORDER BY post_created_time DESC
            """, (month,))
            return cursor.fetchall()
    
    def get_monthly_page_stats(self, month: date, page_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get aggregated monthly stats per page."""
        with self.get_cursor() as cursor:
            query = "SELECT * FROM view_fb_monthly_page_stats WHERE month = %s"
            params = [month]
            
            if page_id:
                query += " AND page_id = %s"
                params.append(page_id)
            
            cursor.execute(query, params)
            return cursor.fetchall()
    
    # ==================== Stats ====================
    
    def get_snapshot_count(self) -> int:
        """Get total number of metrics snapshots."""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM fb_post_metrics")
            result = cursor.fetchone()
            return result["count"] if result else 0
    
    def get_post_count(self) -> int:
        """Get total number of posts."""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM fb_posts")
            result = cursor.fetchone()
            return result["count"] if result else 0
