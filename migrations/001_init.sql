-- SocialDash Meta Cache - Database Schema
-- Migration 001: Initial Tables

-- Facebook Pages
CREATE TABLE IF NOT EXISTS fb_pages (
    page_id TEXT PRIMARY KEY,
    name TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Facebook Posts
CREATE TABLE IF NOT EXISTS fb_posts (
    post_id TEXT PRIMARY KEY,
    page_id TEXT NOT NULL REFERENCES fb_pages(page_id) ON DELETE CASCADE,
    created_time TIMESTAMPTZ NOT NULL,
    type TEXT,
    permalink TEXT,
    message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Facebook Post Metrics (Time Series Snapshots)
CREATE TABLE IF NOT EXISTS fb_post_metrics (
    id SERIAL PRIMARY KEY,
    post_id TEXT NOT NULL REFERENCES fb_posts(post_id) ON DELETE CASCADE,
    snapshot_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reactions_total INTEGER DEFAULT 0,
    comments_total INTEGER DEFAULT 0,
    shares_total INTEGER,
    reach INTEGER,
    impressions INTEGER,
    video_3s_views INTEGER,
    shares_limited BOOLEAN DEFAULT TRUE,
    raw_json JSONB
);

-- Monthly Post Summary (Optional, for finalized reports)
CREATE TABLE IF NOT EXISTS fb_monthly_post_summary (
    id SERIAL PRIMARY KEY,
    month DATE NOT NULL,
    post_id TEXT NOT NULL REFERENCES fb_posts(post_id) ON DELETE CASCADE,
    page_id TEXT NOT NULL REFERENCES fb_pages(page_id) ON DELETE CASCADE,
    reach INTEGER,
    impressions INTEGER,
    reactions_total INTEGER DEFAULT 0,
    comments_total INTEGER DEFAULT 0,
    video_3s_views INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(month, post_id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fb_posts_page_created ON fb_posts(page_id, created_time);
CREATE INDEX IF NOT EXISTS idx_fb_post_metrics_post_snapshot ON fb_post_metrics(post_id, snapshot_time DESC);
CREATE INDEX IF NOT EXISTS idx_fb_monthly_summary_month_page ON fb_monthly_post_summary(month, page_id);
CREATE INDEX IF NOT EXISTS idx_fb_posts_created_time ON fb_posts(created_time);
