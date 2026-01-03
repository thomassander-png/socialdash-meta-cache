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

    
    # ==================== Instagram Accounts ====================
    
    def upsert_ig_account(
        self,
        account_id: str,
        username: str,
        name: Optional[str] = None,
        linked_fb_page_id: Optional[str] = None
    ) -> None:
        """Insert or update an Instagram Business Account."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO ig_accounts (account_id, username, name, linked_fb_page_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (account_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    name = COALESCE(EXCLUDED.name, ig_accounts.name),
                    linked_fb_page_id = COALESCE(EXCLUDED.linked_fb_page_id, ig_accounts.linked_fb_page_id)
            """, (account_id, username, name, linked_fb_page_id))
        
        logger.debug(f"Upserted IG account: {account_id} (@{username})")
    
    def get_ig_accounts(self) -> List[Dict[str, Any]]:
        """Get all tracked Instagram accounts."""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT * FROM ig_accounts ORDER BY username")
            return cursor.fetchall()
    
    # ==================== Customer Accounts ====================
    
    def upsert_customer_account(
        self,
        platform: str,
        account_id: str,
        account_name: Optional[str] = None,
        customer_id: Optional[str] = None
    ) -> None:
        """Insert or update a customer account mapping."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO customer_accounts (platform, account_id, account_name, customer_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (platform, account_id) DO UPDATE SET
                    account_name = COALESCE(EXCLUDED.account_name, customer_accounts.account_name),
                    updated_at = NOW()
            """, (platform, account_id, account_name, customer_id))
        
        logger.debug(f"Upserted customer_account: {platform}/{account_id}")
    
    def get_customer_accounts(
        self,
        customer_id: Optional[str] = None,
        platform: Optional[str] = None,
        unassigned_only: bool = False
    ) -> List[Dict[str, Any]]:
        """Get customer accounts with optional filters."""
        with self.get_cursor() as cursor:
            query = "SELECT * FROM customer_accounts WHERE 1=1"
            params = []
            
            if customer_id:
                query += " AND customer_id = %s"
                params.append(customer_id)
            
            if platform:
                query += " AND platform = %s"
                params.append(platform)
            
            if unassigned_only:
                query += " AND customer_id IS NULL"
            
            query += " ORDER BY platform, account_name"
            
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def assign_account_to_customer(
        self,
        account_id: str,
        platform: str,
        customer_id: Optional[str]
    ) -> bool:
        """Assign an account to a customer (or unassign if customer_id is None)."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                UPDATE customer_accounts
                SET customer_id = %s, updated_at = NOW()
                WHERE account_id = %s AND platform = %s
                RETURNING id
            """, (customer_id, account_id, platform))
            
            result = cursor.fetchone()
            return result is not None
    
    # ==================== Customers ====================
    
    def create_customer(self, name: str) -> Optional[str]:
        """Create a new customer and return the customer_id."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO customers (name)
                VALUES (%s)
                RETURNING customer_id
            """, (name,))
            
            result = cursor.fetchone()
            customer_id = str(result["customer_id"]) if result else None
        
        logger.info(f"Created customer: {name} ({customer_id})")
        return customer_id
    
    def get_customers(self, active_only: bool = False) -> List[Dict[str, Any]]:
        """Get all customers."""
        with self.get_cursor() as cursor:
            query = "SELECT * FROM view_customer_summary"
            if active_only:
                query += " WHERE is_active = true"
            query += " ORDER BY name"
            
            cursor.execute(query)
            return cursor.fetchall()
    
    def get_customer(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """Get a single customer by ID."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM view_customer_summary
                WHERE customer_id = %s
            """, (customer_id,))
            return cursor.fetchone()
    
    def update_customer(
        self,
        customer_id: str,
        name: Optional[str] = None,
        is_active: Optional[bool] = None
    ) -> bool:
        """Update customer details."""
        updates = []
        params = []
        
        if name is not None:
            updates.append("name = %s")
            params.append(name)
        
        if is_active is not None:
            updates.append("is_active = %s")
            params.append(is_active)
        
        if not updates:
            return False
        
        updates.append("updated_at = NOW()")
        params.append(customer_id)
        
        with self.get_cursor() as cursor:
            cursor.execute(f"""
                UPDATE customers
                SET {', '.join(updates)}
                WHERE customer_id = %s
                RETURNING customer_id
            """, params)
            
            result = cursor.fetchone()
            return result is not None
    
    def delete_customer(self, customer_id: str) -> bool:
        """Delete a customer (only if no accounts assigned)."""
        with self.get_cursor() as cursor:
            # Check if any accounts are assigned
            cursor.execute("""
                SELECT COUNT(*) as count FROM customer_accounts
                WHERE customer_id = %s
            """, (customer_id,))
            
            result = cursor.fetchone()
            if result and result["count"] > 0:
                logger.warning(f"Cannot delete customer {customer_id}: has assigned accounts")
                return False
            
            cursor.execute("""
                DELETE FROM customers
                WHERE customer_id = %s
                RETURNING customer_id
            """, (customer_id,))
            
            result = cursor.fetchone()
            return result is not None
    
    # ==================== Reports ====================
    
    def create_report(
        self,
        customer_id: str,
        month: date,
        status: str = "pending"
    ) -> Optional[str]:
        """Create a new report entry."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO reports (customer_id, month, status)
                VALUES (%s, %s, %s)
                ON CONFLICT (customer_id, month) DO UPDATE SET
                    status = EXCLUDED.status,
                    updated_at = NOW()
                RETURNING report_id
            """, (customer_id, month, status))
            
            result = cursor.fetchone()
            return str(result["report_id"]) if result else None
    
    def update_report(
        self,
        report_id: str,
        status: Optional[str] = None,
        pptx_url: Optional[str] = None,
        pdf_url: Optional[str] = None,
        error_message: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Update report details."""
        updates = ["updated_at = NOW()"]
        params = []
        
        if status is not None:
            updates.append("status = %s")
            params.append(status)
            if status == "generated":
                updates.append("generated_at = NOW()")
        
        if pptx_url is not None:
            updates.append("pptx_url = %s")
            params.append(pptx_url)
        
        if pdf_url is not None:
            updates.append("pdf_url = %s")
            params.append(pdf_url)
        
        if error_message is not None:
            updates.append("error_message = %s")
            params.append(error_message)
        
        if meta is not None:
            updates.append("meta = %s")
            params.append(json.dumps(meta))
        
        params.append(report_id)
        
        with self.get_cursor() as cursor:
            cursor.execute(f"""
                UPDATE reports
                SET {', '.join(updates)}
                WHERE report_id = %s
                RETURNING report_id
            """, params)
            
            result = cursor.fetchone()
            return result is not None
    
    def get_reports(
        self,
        customer_id: Optional[str] = None,
        month: Optional[date] = None,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get reports with optional filters."""
        with self.get_cursor() as cursor:
            query = "SELECT * FROM view_customer_reports WHERE 1=1"
            params = []
            
            if customer_id:
                query += " AND customer_id = %s"
                params.append(customer_id)
            
            if month:
                query += " AND month = %s"
                params.append(month)
            
            if status:
                query += " AND status = %s"
                params.append(status)
            
            query += " ORDER BY month DESC, customer_name"
            
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def get_report(self, report_id: str) -> Optional[Dict[str, Any]]:
        """Get a single report by ID."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM view_customer_reports
                WHERE report_id = %s
            """, (report_id,))
            return cursor.fetchone()
