"""
Module for finalizing monthly reports.
Creates summary records from the last snapshot of each post in a given month.
"""

import logging
from datetime import date, datetime
from typing import Dict, List, Optional

from .config import Config
from .db import Database

logger = logging.getLogger(__name__)


class MonthFinalizer:
    """Handles finalization of monthly post summaries."""
    
    def __init__(self, config: Config, db: Database):
        self.config = config
        self.db = db
    
    def finalize_month(self, month: date) -> Dict[str, int]:
        """
        Finalize a month by creating summary records.
        
        Uses the view_fb_monthly_post_metrics view to get the last
        snapshot for each post in the given month, then writes
        to fb_monthly_post_summary table.
        
        Args:
            month: First day of the month to finalize (e.g., date(2025, 12, 1))
        
        Returns:
            Dict with counts: {"posts_processed": N, "summaries_created": N}
        """
        logger.info(f"Finalizing month: {month.strftime('%Y-%m')}")
        
        # Get monthly metrics from view
        monthly_metrics = self.db.get_monthly_post_metrics(month)
        
        if not monthly_metrics:
            logger.warning(f"No metrics found for month {month.strftime('%Y-%m')}")
            return {"posts_processed": 0, "summaries_created": 0}
        
        logger.info(f"Found {len(monthly_metrics)} posts with metrics for {month.strftime('%Y-%m')}")
        
        summaries_created = 0
        
        for metrics in monthly_metrics:
            try:
                self.db.upsert_monthly_summary(
                    month=month,
                    post_id=metrics["post_id"],
                    page_id=metrics["page_id"],
                    reach=metrics.get("reach"),
                    impressions=metrics.get("impressions"),
                    reactions_total=metrics.get("reactions_total", 0),
                    comments_total=metrics.get("comments_total", 0),
                    video_3s_views=metrics.get("video_3s_views")
                )
                summaries_created += 1
                
            except Exception as e:
                logger.error(f"Failed to create summary for post {metrics['post_id']}: {e}")
        
        logger.info(f"Month {month.strftime('%Y-%m')} finalized: {summaries_created} summaries created")
        
        return {
            "posts_processed": len(monthly_metrics),
            "summaries_created": summaries_created
        }
    
    def get_month_summary(self, month: date, page_id: Optional[str] = None) -> Dict:
        """
        Get aggregated summary for a month.
        
        Returns:
            Dict with aggregated stats
        """
        stats = self.db.get_monthly_page_stats(month, page_id)
        
        if not stats:
            return {
                "month": month.strftime("%Y-%m"),
                "total_posts": 0,
                "total_reactions": 0,
                "total_comments": 0,
                "total_interactions": 0,
                "total_reach": 0,
                "avg_reach_per_post": 0,
                "pages": []
            }
        
        # Aggregate across all pages
        total_posts = sum(s["total_posts"] for s in stats)
        total_reactions = sum(s["total_reactions"] for s in stats)
        total_comments = sum(s["total_comments"] for s in stats)
        total_interactions = sum(s["total_interactions"] for s in stats)
        total_reach = sum(s["total_reach"] for s in stats)
        
        avg_reach = total_reach / total_posts if total_posts > 0 else 0
        
        return {
            "month": month.strftime("%Y-%m"),
            "total_posts": total_posts,
            "total_reactions": total_reactions,
            "total_comments": total_comments,
            "total_interactions": total_interactions,
            "total_reach": total_reach,
            "avg_reach_per_post": round(avg_reach, 2),
            "pages": [dict(s) for s in stats]
        }


def run_finalize_month(config: Config, month: date) -> Dict[str, int]:
    """
    Main entry point for finalizing a month.
    
    Args:
        config: Application configuration
        month: First day of the month to finalize
    
    Returns:
        Dict with finalization results
    """
    db = Database(config)
    finalizer = MonthFinalizer(config, db)
    
    return finalizer.finalize_month(month)


def parse_month_string(month_str: str) -> date:
    """
    Parse a month string into a date object.
    
    Accepts formats:
    - "2025-12" -> date(2025, 12, 1)
    - "2025-12-01" -> date(2025, 12, 1)
    - "December 2025" -> date(2025, 12, 1)
    """
    # Try YYYY-MM format
    try:
        dt = datetime.strptime(month_str, "%Y-%m")
        return dt.date().replace(day=1)
    except ValueError:
        pass
    
    # Try YYYY-MM-DD format
    try:
        dt = datetime.strptime(month_str, "%Y-%m-%d")
        return dt.date().replace(day=1)
    except ValueError:
        pass
    
    # Try "Month Year" format
    try:
        dt = datetime.strptime(month_str, "%B %Y")
        return dt.date().replace(day=1)
    except ValueError:
        pass
    
    raise ValueError(f"Could not parse month string: {month_str}")
