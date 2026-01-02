-- Migration 004: Add media URL columns for thumbnail caching
-- This enables the PPTX report generator to use cached thumbnails

-- Extend fb_posts with media URL columns
ALTER TABLE fb_posts ADD COLUMN IF NOT EXISTS media_url TEXT;
ALTER TABLE fb_posts ADD COLUMN IF NOT EXISTS thumbnail_url TEXT;
ALTER TABLE fb_posts ADD COLUMN IF NOT EXISTS og_image_url TEXT;
ALTER TABLE fb_posts ADD COLUMN IF NOT EXISTS preview_source TEXT; -- 'graph_api' | 'open_graph' | 'none'

-- Extend ig_posts with preview_source (media_url and thumbnail_url already exist from 003)
ALTER TABLE ig_posts ADD COLUMN IF NOT EXISTS preview_source TEXT; -- 'graph_api' | 'open_graph' | 'none'

-- Update views to include media URLs

-- Update Facebook view to include media URLs
CREATE OR REPLACE VIEW view_fb_post_latest_metrics AS
SELECT DISTINCT ON (m.post_id)
    m.post_id,
    p.page_id,
    p.created_time AS post_created_time,
    p.type AS post_type,
    p.permalink,
    p.message,
    p.media_url,
    p.thumbnail_url,
    p.og_image_url,
    p.preview_source,
    m.snapshot_time,
    m.reactions_total,
    m.comments_total,
    m.shares_total,
    m.reach,
    m.impressions,
    m.video_3s_views,
    m.shares_limited,
    (COALESCE(m.reactions_total, 0) + COALESCE(m.comments_total, 0)) AS interactions_total
FROM fb_post_metrics m
JOIN fb_posts p ON m.post_id = p.post_id
ORDER BY m.post_id, m.snapshot_time DESC;

-- Update Facebook monthly view to include media URLs
CREATE OR REPLACE VIEW view_fb_monthly_post_metrics AS
SELECT DISTINCT ON (DATE_TRUNC('month', p.created_time), m.post_id)
    DATE_TRUNC('month', p.created_time)::DATE AS month,
    m.post_id,
    p.page_id,
    p.created_time AS post_created_time,
    p.type AS post_type,
    p.permalink,
    p.message,
    p.media_url,
    p.thumbnail_url,
    p.og_image_url,
    p.preview_source,
    m.snapshot_time,
    m.reactions_total,
    m.comments_total,
    m.shares_total,
    m.reach,
    m.impressions,
    m.video_3s_views,
    m.shares_limited,
    (COALESCE(m.reactions_total, 0) + COALESCE(m.comments_total, 0)) AS interactions_total
FROM fb_post_metrics m
JOIN fb_posts p ON m.post_id = p.post_id
ORDER BY DATE_TRUNC('month', p.created_time), m.post_id, m.snapshot_time DESC;

-- Update Instagram view to include preview_source
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
    p.preview_source,
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

-- Update Instagram monthly view to include preview_source
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
    p.preview_source,
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

-- Comments for documentation
COMMENT ON COLUMN fb_posts.media_url IS 'Direct media URL from Graph API attachments';
COMMENT ON COLUMN fb_posts.thumbnail_url IS 'Thumbnail URL (from Graph API or OpenGraph)';
COMMENT ON COLUMN fb_posts.og_image_url IS 'OpenGraph image URL extracted from permalink';
COMMENT ON COLUMN fb_posts.preview_source IS 'Source of preview image: graph_api, open_graph, or none';
COMMENT ON COLUMN ig_posts.preview_source IS 'Source of preview image: graph_api, open_graph, or none';
