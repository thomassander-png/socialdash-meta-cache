-- Migration 003: Instagram Tables and Views
-- Run this after 001_init.sql and 002_views.sql

-- Instagram Accounts
CREATE TABLE IF NOT EXISTS ig_accounts (
    account_id TEXT PRIMARY KEY,
    username TEXT,
    name TEXT,
    followers_count INTEGER,
    media_count INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Instagram Posts/Media
CREATE TABLE IF NOT EXISTS ig_posts (
    media_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES ig_accounts(account_id),
    media_type TEXT NOT NULL, -- IMAGE, VIDEO, CAROUSEL_ALBUM, REEL
    caption TEXT,
    permalink TEXT,
    timestamp TIMESTAMPTZ NOT NULL,
    media_url TEXT,
    thumbnail_url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Instagram Post Metrics (Snapshots)
CREATE TABLE IF NOT EXISTS ig_post_metrics (
    id SERIAL PRIMARY KEY,
    media_id TEXT NOT NULL REFERENCES ig_posts(media_id),
    snapshot_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    likes INTEGER DEFAULT 0,
    comments INTEGER DEFAULT 0,
    saves INTEGER,
    reach INTEGER,
    impressions INTEGER,
    plays INTEGER, -- for videos/reels
    shares INTEGER,
    profile_visits INTEGER,
    raw_json JSONB
);

-- Instagram Monthly Summary (optional, for finalize_month)
CREATE TABLE IF NOT EXISTS ig_monthly_post_summary (
    id SERIAL PRIMARY KEY,
    month DATE NOT NULL,
    media_id TEXT NOT NULL REFERENCES ig_posts(media_id),
    account_id TEXT NOT NULL,
    reach INTEGER,
    impressions INTEGER,
    likes INTEGER,
    comments INTEGER,
    saves INTEGER,
    plays INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(month, media_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_ig_posts_account_timestamp ON ig_posts(account_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_ig_post_metrics_media_snapshot ON ig_post_metrics(media_id, snapshot_time DESC);
CREATE INDEX IF NOT EXISTS idx_ig_monthly_summary_month_account ON ig_monthly_post_summary(month, account_id);

-- View: Latest metrics per IG post
CREATE OR REPLACE VIEW view_ig_post_latest_metrics AS
SELECT DISTINCT ON (m.media_id)
    m.media_id,
    p.account_id,
    p.timestamp AS post_created_time,
    p.media_type,
    p.permalink,
    p.caption,
    p.media_url,
    p.thumbnail_url,
    m.snapshot_time,
    m.likes,
    m.comments,
    m.saves,
    m.reach,
    m.impressions,
    m.plays,
    m.shares,
    (COALESCE(m.likes, 0) + COALESCE(m.comments, 0) + COALESCE(m.saves, 0)) AS interactions_total
FROM ig_post_metrics m
JOIN ig_posts p ON m.media_id = p.media_id
ORDER BY m.media_id, m.snapshot_time DESC;

-- View: Monthly IG post metrics (last snapshot per post per month)
CREATE OR REPLACE VIEW view_ig_monthly_post_metrics AS
SELECT DISTINCT ON (DATE_TRUNC('month', p.timestamp), m.media_id)
    DATE_TRUNC('month', p.timestamp)::DATE AS month,
    m.media_id,
    p.account_id,
    p.timestamp AS post_created_time,
    p.media_type,
    p.permalink,
    p.caption,
    p.media_url,
    p.thumbnail_url,
    m.snapshot_time,
    m.likes,
    m.comments,
    m.saves,
    m.reach,
    m.impressions,
    m.plays,
    m.shares,
    (COALESCE(m.likes, 0) + COALESCE(m.comments, 0) + COALESCE(m.saves, 0)) AS interactions_total
FROM ig_post_metrics m
JOIN ig_posts p ON m.media_id = p.media_id
ORDER BY DATE_TRUNC('month', p.timestamp), m.media_id, m.snapshot_time DESC;

-- View: Monthly IG account stats aggregation
CREATE OR REPLACE VIEW view_ig_monthly_account_stats AS
SELECT 
    month,
    account_id,
    COUNT(*) AS total_posts,
    SUM(likes) AS total_likes,
    SUM(comments) AS total_comments,
    SUM(COALESCE(saves, 0)) AS total_saves,
    SUM(COALESCE(reach, 0)) AS total_reach,
    SUM(COALESCE(impressions, 0)) AS total_impressions,
    SUM(COALESCE(plays, 0)) AS total_plays,
    SUM(interactions_total) AS total_interactions,
    ROUND(AVG(COALESCE(reach, 0))) AS avg_reach_per_post,
    ROUND(AVG(interactions_total)::numeric, 2) AS avg_interactions_per_post
FROM view_ig_monthly_post_metrics
GROUP BY month, account_id;
