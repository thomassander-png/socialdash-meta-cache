# SocialDash Meta Cache

A cost-effective, automated caching system for Meta/Facebook Graph API data. This collector runs via GitHub Actions to regularly fetch and store Facebook Page posts and metrics in a PostgreSQL database, enabling reproducible monthly reports independent of Meta API data retention limits.

## Features

- **Automated Data Collection**: GitHub Actions cron job runs every 6 hours
- **Time Series Metrics**: Stores snapshots of post metrics (reactions, comments, shares, reach, impressions)
- **Monthly Reporting**: Finalize monthly summaries for reproducible reports
- **Rate Limit Handling**: Exponential backoff for API rate limits
- **Robust Pagination**: Handles large result sets reliably
- **Secure**: No tokens in logs, secrets via GitHub Secrets

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  GitHub Actions │────▶│  Meta Graph API  │────▶│  PostgreSQL DB  │
│  (Scheduler)    │     │  (Facebook)      │     │  (Supabase)     │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                                                          │
                                                          ▼
                                                 ┌─────────────────┐
                                                 │  Dashboard      │
                                                 │  (Next.js)      │
                                                 └─────────────────┘
```

## Setup

### 1. Database Setup (Supabase)

1. Create a free Supabase project at [supabase.com](https://supabase.com)
2. Go to **SQL Editor** and run the migrations:
   - Copy contents of `migrations/001_init.sql` and execute
   - Copy contents of `migrations/002_views.sql` and execute
3. Get your connection string from **Settings > Database > Connection string (URI)**

### 2. Facebook Access Token

You need a **Page Access Token** with the following permissions:
- `pages_read_engagement`
- `pages_read_user_content`
- `read_insights` (for reach/impressions)

**To get a long-lived token:**
1. Go to [Facebook Developer Portal](https://developers.facebook.com)
2. Create an app or use existing one
3. Use Graph API Explorer to generate a Page Access Token
4. Exchange for long-lived token (60 days)

### 3. GitHub Secrets

Configure these secrets in your repository (**Settings > Secrets and variables > Actions**):

| Secret | Required | Description |
|--------|----------|-------------|
| `META_ACCESS_TOKEN` | ✅ | Facebook Page Access Token |
| `DATABASE_URL` | ✅ | PostgreSQL connection string |
| `FB_PAGE_IDS` | ✅ | Comma-separated Page IDs (e.g., `123456789,987654321`) |
| `META_API_VERSION` | ❌ | API version (default: `v20.0`) |
| `TZ` | ❌ | Timezone (default: `Europe/Berlin`) |

### 4. Enable GitHub Actions

The workflow runs automatically every 6 hours. You can also trigger it manually.

## Usage

### Automatic Caching (Default)

The GitHub Action runs every 6 hours and:
1. Fetches posts from the last 45 days
2. Stores/updates posts in `fb_posts` table
3. Creates new metrics snapshots in `fb_post_metrics` table

### Manual Operations

Go to **Actions > Cache Meta Data > Run workflow** and select:

#### Cache (Default)
Standard operation - fetches recent posts and metrics.

#### Backfill
Fetch historical data for a specific date range:
- `start_date`: e.g., `2025-12-01`
- `end_date`: e.g., `2025-12-31`

#### Finalize Month
Create monthly summary records for reporting:
- `month`: e.g., `2025-12` or `2025-12-01`

This writes to `fb_monthly_post_summary` table with the last known metrics for each post in that month.

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export META_ACCESS_TOKEN="your_token"
export DATABASE_URL="postgresql://..."
export FB_PAGE_IDS="123456789"

# Run cache operation
python -m src.main --mode cache

# Run backfill
python -m src.main --mode backfill --start 2025-12-01 --end 2025-12-31

# Finalize December 2025
python -m src.main --mode finalize_month --month 2025-12

# Run migrations
python -m src.main --mode migrate
```

## Database Schema

### Tables

| Table | Description |
|-------|-------------|
| `fb_pages` | Facebook Pages being tracked |
| `fb_posts` | Posts from tracked pages |
| `fb_post_metrics` | Time series metrics snapshots |
| `fb_monthly_post_summary` | Finalized monthly summaries |

### Views

| View | Description |
|------|-------------|
| `view_fb_post_latest_metrics` | Latest metrics per post |
| `view_fb_monthly_post_metrics` | Last snapshot per post per month |
| `view_fb_monthly_page_stats` | Aggregated monthly stats per page |

## API Limitations

### Shares
- Shares count may not be available for all posts
- When unavailable, `shares_total` is `NULL` and `shares_limited` is `TRUE`
- **Do not include shares in interaction calculations**

### Saves
- **Not available** via Graph API
- Cannot be tracked or reported

### Reach & Impressions
- Requires `read_insights` permission
- May not be available for all post types
- Stored as `NULL` when unavailable

### Data Retention
- Facebook Insights data has retention limits (typically 2 years)
- This system caches data to preserve it beyond API limits
- **Run regular caching to capture data before it expires**

## Finalize December 2025

To create a finalized report for December 2025:

1. Go to **Actions > Cache Meta Data > Run workflow**
2. Select `mode: finalize_month`
3. Enter `month: 2025-12`
4. Click **Run workflow**

This creates summary records in `fb_monthly_post_summary` that can be queried by the dashboard.

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

## Troubleshooting

### Token Expired
Facebook tokens expire. Check the workflow logs for authentication errors and regenerate the token.

### Rate Limits
The collector implements exponential backoff. If you see rate limit errors, the system will automatically retry.

### Missing Insights
Not all posts have insights available. This is normal for:
- Very old posts
- Posts without sufficient engagement
- Certain post types

## License

MIT
