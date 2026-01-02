"""
Main CLI entry point for SocialDash Meta Cache.

Usage:
    python -m src.main --mode cache
    python -m src.main --mode cache_ig
    python -m src.main --mode cache_all
    python -m src.main --mode backfill --start 2025-12-01 --end 2025-12-31
    python -m src.main --mode finalize_month --month 2025-12-01
    python -m src.main --mode migrate
    python -m src.main --mode report --client "ClientName" --month 2025-12
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
    """Run the cache operation (posts + metrics) for Facebook."""
    logger.info("=" * 60)
    logger.info("Starting Facebook cache operation")
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
    logger.info("FACEBOOK CACHE OPERATION COMPLETE")
    logger.info(f"Pages processed: {len(config.fb_page_ids)}")
    logger.info(f"Posts cached: {total_posts}")
    logger.info(f"Metrics snapshots: {total_success} success, {total_failed} failed")
    logger.info("=" * 60)
    
    return {
        "pages": len(config.fb_page_ids),
        "posts": total_posts,
        "metrics_success": total_success,
        "metrics_failed": total_failed
    }


def run_cache_instagram(config: Config, since: Optional[datetime] = None, until: Optional[datetime] = None):
    """Run the cache operation for Instagram."""
    from .cache_instagram import run_instagram_cache
    
    logger.info("=" * 60)
    logger.info("Starting Instagram cache operation")
    logger.info(f"Token: {config.get_masked_token()}")
    logger.info(f"Accounts: {', '.join(config.ig_account_ids)}")
    logger.info(f"API Version: {config.meta_api_version}")
    logger.info("=" * 60)
    
    start_date = since.strftime("%Y-%m-%d") if since else None
    end_date = until.strftime("%Y-%m-%d") if until else None
    
    result = run_instagram_cache(
        days_back=config.lookback_days,
        start_date=start_date,
        end_date=end_date
    )
    
    logger.info("=" * 60)
    logger.info("INSTAGRAM CACHE OPERATION COMPLETE")
    logger.info(f"Accounts cached: {result.get('accounts_cached', 0)}")
    logger.info(f"Posts cached: {result.get('posts_cached', 0)}")
    logger.info(f"Metrics snapshots: {result.get('metrics_cached', 0)}")
    logger.info("=" * 60)
    
    return result


def run_cache_all(config: Config, since: Optional[datetime] = None, until: Optional[datetime] = None):
    """Run cache operation for both Facebook and Instagram."""
    logger.info("=" * 60)
    logger.info("Starting FULL cache operation (Facebook + Instagram)")
    logger.info("=" * 60)
    
    fb_result = {}
    ig_result = {}
    
    # Facebook
    if config.fb_page_ids:
        fb_result = run_cache(config, since=since, until=until)
    else:
        logger.warning("No Facebook Page IDs configured, skipping Facebook")
    
    # Instagram
    if config.ig_account_ids:
        ig_result = run_cache_instagram(config, since=since, until=until)
    else:
        logger.warning("No Instagram Account IDs configured, skipping Instagram")
    
    logger.info("=" * 60)
    logger.info("FULL CACHE OPERATION COMPLETE")
    logger.info("=" * 60)
    
    return {
        "facebook": fb_result,
        "instagram": ig_result
    }


def run_backfill(config: Config, start: datetime, end: datetime):
    """Run backfill operation for a specific date range."""
    logger.info("=" * 60)
    logger.info("Starting backfill operation")
    logger.info(f"Date range: {start.date()} to {end.date()}")
    logger.info(f"Pages: {', '.join(config.fb_page_ids)}")
    logger.info("=" * 60)
    
    return run_cache_all(config, since=start, until=end)


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


def run_report(client_name: str, report_month: str, output_dir: str = "/tmp/reports"):
    """Generate PPTX report."""
    from .report_generator import generate_report
    
    logger.info("=" * 60)
    logger.info(f"Generating report for {client_name} - {report_month}")
    logger.info("=" * 60)
    
    report_path = generate_report(
        client_name=client_name,
        report_month=report_month,
        output_dir=output_dir
    )
    
    logger.info("=" * 60)
    logger.info("REPORT GENERATION COMPLETE")
    logger.info(f"Report saved to: {report_path}")
    logger.info("=" * 60)
    
    return {"report_path": report_path}


def main():
    parser = argparse.ArgumentParser(
        description="SocialDash Meta Cache - Facebook & Instagram data collector"
    )
    
    parser.add_argument(
        "--mode",
        choices=["cache", "cache_ig", "cache_all", "backfill", "finalize_month", "migrate", "report"],
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
        help="Month to finalize or report (YYYY-MM or YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--client",
        type=str,
        help="Client name for report generation"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default="/tmp/reports",
        help="Output directory for reports"
    )
    
    args = parser.parse_args()
    
    try:
        config = get_config()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    
    try:
        if args.mode == "cache":
            config.validate(require_fb=True, require_ig=False)
            result = run_cache(config)
            
        elif args.mode == "cache_ig":
            config.validate(require_fb=False, require_ig=True)
            result = run_cache_instagram(config)
            
        elif args.mode == "cache_all":
            config.validate(require_fb=False, require_ig=False)
            result = run_cache_all(config)
            
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
            
        elif args.mode == "report":
            if not args.client or not args.month:
                logger.error("Report mode requires --client and --month parameters")
                sys.exit(1)
            
            result = run_report(args.client, args.month, args.output)
        
        # Exit successfully
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Operation failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
