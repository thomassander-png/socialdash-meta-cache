"""
Main CLI entry point for SocialDash Meta Cache.

Usage:
    python -m src.main --mode cache
    python -m src.main --mode backfill --start 2025-12-01 --end 2025-12-31
    python -m src.main --mode finalize_month --month 2025-12-01
    python -m src.main --mode migrate
"""

import argparse
import logging
import sys
from datetime import datetime, date
from typing import Optional

from .config import get_config, Config
from .cache_posts import run_cache_posts
from .cache_metrics import run_cache_metrics
from .finalize_month import run_finalize_month, parse_month_string
from .db import Database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> datetime:
    """Parse a date string into a datetime object."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")


def run_cache(config: Config, since: Optional[datetime] = None, until: Optional[datetime] = None):
    """Run the cache operation (posts + metrics)."""
    logger.info("=" * 60)
    logger.info("Starting cache operation")
    logger.info(f"Token: {config.get_masked_token()}")
    logger.info(f"Pages: {', '.join(config.fb_page_ids)}")
    logger.info(f"API Version: {config.meta_api_version}")
    logger.info("=" * 60)
    
    # Step 1: Cache posts
    logger.info("Step 1: Caching posts...")
    post_results = run_cache_posts(config, since=since, until=until)
    
    total_posts = sum(r.get("upserted", 0) for r in post_results.values())
    logger.info(f"Posts cached: {total_posts}")
    
    # Step 2: Cache metrics for those posts
    logger.info("Step 2: Caching metrics...")
    metrics_results = run_cache_metrics(config, since=since, until=until)
    
    total_success = sum(r.get("success", 0) for r in metrics_results.values())
    total_failed = sum(r.get("failed", 0) for r in metrics_results.values())
    
    # Summary
    logger.info("=" * 60)
    logger.info("CACHE OPERATION COMPLETE")
    logger.info(f"Pages processed: {len(config.fb_page_ids)}")
    logger.info(f"Posts cached: {total_posts}")
    logger.info(f"Metrics snapshots: {total_success} success, {total_failed} failed")
    logger.info("=" * 60)
    
    # Return summary for GitHub Actions
    return {
        "pages": len(config.fb_page_ids),
        "posts": total_posts,
        "metrics_success": total_success,
        "metrics_failed": total_failed
    }


def run_backfill(config: Config, start: datetime, end: datetime):
    """Run backfill operation for a specific date range."""
    logger.info("=" * 60)
    logger.info("Starting backfill operation")
    logger.info(f"Date range: {start.date()} to {end.date()}")
    logger.info(f"Pages: {', '.join(config.fb_page_ids)}")
    logger.info("=" * 60)
    
    return run_cache(config, since=start, until=end)


def run_finalize(config: Config, month_str: str):
    """Run month finalization."""
    month = parse_month_string(month_str)
    
    logger.info("=" * 60)
    logger.info(f"Finalizing month: {month.strftime('%Y-%m')}")
    logger.info("=" * 60)
    
    result = run_finalize_month(config, month)
    
    logger.info("=" * 60)
    logger.info("FINALIZATION COMPLETE")
    logger.info(f"Posts processed: {result['posts_processed']}")
    logger.info(f"Summaries created: {result['summaries_created']}")
    logger.info("=" * 60)
    
    return result


def run_migrate(config: Config):
    """Run database migrations."""
    logger.info("=" * 60)
    logger.info("Running database migrations")
    logger.info("=" * 60)
    
    db = Database(config)
    db.run_migrations()
    
    logger.info("Migrations complete")


def main():
    parser = argparse.ArgumentParser(
        description="SocialDash Meta Cache - Facebook data collector"
    )
    
    parser.add_argument(
        "--mode",
        choices=["cache", "backfill", "finalize_month", "migrate"],
        required=True,
        help="Operation mode"
    )
    
    parser.add_argument(
        "--start",
        type=str,
        help="Start date for backfill (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end",
        type=str,
        help="End date for backfill (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--month",
        type=str,
        help="Month to finalize (YYYY-MM or YYYY-MM-DD)"
    )
    
    args = parser.parse_args()
    
    try:
        config = get_config()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    
    try:
        if args.mode == "cache":
            result = run_cache(config)
            
        elif args.mode == "backfill":
            if not args.start or not args.end:
                logger.error("Backfill mode requires --start and --end dates")
                sys.exit(1)
            
            start = parse_date(args.start)
            end = parse_date(args.end)
            result = run_backfill(config, start, end)
            
        elif args.mode == "finalize_month":
            if not args.month:
                logger.error("Finalize mode requires --month parameter")
                sys.exit(1)
            
            result = run_finalize(config, args.month)
            
        elif args.mode == "migrate":
            run_migrate(config)
            result = {"status": "complete"}
        
        # Exit successfully
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Operation failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
