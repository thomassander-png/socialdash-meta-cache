-- SocialDash Meta Cache - Database Views
-- Migration 002: Views for reporting

-- View: Latest metrics per post (most recent snapshot)
CREATE OR REPLACE VIEW view_fb_post_latest_metrics AS
SELECT DISTINCT ON (m.post_id)
    m.post_id,
    p.page_id,
    p.created_time AS post_created_time,
    p.type AS post_type,
    p.permalink,
    p.message,
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

-- View: Monthly post metrics (last snapshot per post per month)
CREATE OR REPLACE VIEW view_fb_monthly_post_metrics AS
SELECT DISTINCT ON (m.post_id, DATE_TRUNC('month', m.snapshot_time))
    m.post_id,
    p.page_id,
    DATE_TRUNC('month', m.snapshot_time)::DATE AS month,
    p.created_time AS post_created_time,
    p.type AS post_type,
    p.permalink,
    p.message,
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
ORDER BY m.post_id, DATE_TRUNC('month', m.snapshot_time), m.snapshot_time DESC;

-- View: Monthly aggregated stats per page
CREATE OR REPLACE VIEW view_fb_monthly_page_stats AS
SELECT 
    page_id,
    month,
    COUNT(DISTINCT post_id) AS total_posts,
    SUM(reactions_total) AS total_reactions,
    SUM(comments_total) AS total_comments,
    SUM(COALESCE(shares_total, 0)) AS total_shares,
    SUM(COALESCE(reach, 0)) AS total_reach,
    SUM(COALESCE(impressions, 0)) AS total_impressions,
    SUM(COALESCE(video_3s_views, 0)) AS total_video_views,
    SUM(interactions_total) AS total_interactions,
    ROUND(AVG(COALESCE(reach, 0))::numeric, 2) AS avg_reach_per_post,
    ROUND(AVG(interactions_total)::numeric, 2) AS avg_interactions_per_post
FROM view_fb_monthly_post_metrics
GROUP BY page_id, month;
