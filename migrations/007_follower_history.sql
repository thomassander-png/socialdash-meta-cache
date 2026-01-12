-- SocialDash Meta Cache - Database Schema
-- Migration 007: Follower History Tables for Growth Tracking

-- Facebook Follower History (Daily Snapshots)
CREATE TABLE IF NOT EXISTS fb_follower_history (
    id SERIAL PRIMARY KEY,
    page_id TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    followers_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(page_id, snapshot_date)
);

-- Instagram Follower History (Daily Snapshots)
CREATE TABLE IF NOT EXISTS ig_follower_history (
    id SERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    followers_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(account_id, snapshot_date)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fb_follower_history_page_date 
    ON fb_follower_history(page_id, snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_ig_follower_history_account_date 
    ON ig_follower_history(account_id, snapshot_date DESC);

-- View: Facebook Follower Growth (Current vs Previous Month)
CREATE OR REPLACE VIEW view_fb_follower_growth AS
SELECT 
    page_id,
    -- Current month's latest follower count
    (SELECT followers_count FROM fb_follower_history fh2 
     WHERE fh2.page_id = fh.page_id 
     AND fh2.snapshot_date >= DATE_TRUNC('month', CURRENT_DATE)
     ORDER BY snapshot_date DESC LIMIT 1) as current_followers,
    -- Previous month's latest follower count
    (SELECT followers_count FROM fb_follower_history fh3 
     WHERE fh3.page_id = fh.page_id 
     AND fh3.snapshot_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
     AND fh3.snapshot_date < DATE_TRUNC('month', CURRENT_DATE)
     ORDER BY snapshot_date DESC LIMIT 1) as previous_followers,
    -- Growth calculation
    COALESCE(
        (SELECT followers_count FROM fb_follower_history fh2 
         WHERE fh2.page_id = fh.page_id 
         AND fh2.snapshot_date >= DATE_TRUNC('month', CURRENT_DATE)
         ORDER BY snapshot_date DESC LIMIT 1), 0
    ) - COALESCE(
        (SELECT followers_count FROM fb_follower_history fh3 
         WHERE fh3.page_id = fh.page_id 
         AND fh3.snapshot_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
         AND fh3.snapshot_date < DATE_TRUNC('month', CURRENT_DATE)
         ORDER BY snapshot_date DESC LIMIT 1), 0
    ) as follower_growth
FROM fb_follower_history fh
GROUP BY page_id;

-- View: Instagram Follower Growth (Current vs Previous Month)
CREATE OR REPLACE VIEW view_ig_follower_growth AS
SELECT 
    account_id,
    -- Current month's latest follower count
    (SELECT followers_count FROM ig_follower_history ih2 
     WHERE ih2.account_id = ih.account_id 
     AND ih2.snapshot_date >= DATE_TRUNC('month', CURRENT_DATE)
     ORDER BY snapshot_date DESC LIMIT 1) as current_followers,
    -- Previous month's latest follower count
    (SELECT followers_count FROM ig_follower_history ih3 
     WHERE ih3.account_id = ih.account_id 
     AND ih3.snapshot_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
     AND ih3.snapshot_date < DATE_TRUNC('month', CURRENT_DATE)
     ORDER BY snapshot_date DESC LIMIT 1) as previous_followers,
    -- Growth calculation
    COALESCE(
        (SELECT followers_count FROM ig_follower_history ih2 
         WHERE ih2.account_id = ih.account_id 
         AND ih2.snapshot_date >= DATE_TRUNC('month', CURRENT_DATE)
         ORDER BY snapshot_date DESC LIMIT 1), 0
    ) - COALESCE(
        (SELECT followers_count FROM ig_follower_history ih3 
         WHERE ih3.account_id = ih.account_id 
         AND ih3.snapshot_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
         AND ih3.snapshot_date < DATE_TRUNC('month', CURRENT_DATE)
         ORDER BY snapshot_date DESC LIMIT 1), 0
    ) as follower_growth
FROM ig_follower_history ih
GROUP BY account_id;

-- Function to get follower growth for a specific month
CREATE OR REPLACE FUNCTION get_fb_follower_growth(
    p_page_id TEXT,
    p_month DATE
) RETURNS TABLE (
    current_followers INTEGER,
    previous_followers INTEGER,
    follower_growth INTEGER,
    growth_percentage NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COALESCE((
            SELECT followers_count FROM fb_follower_history 
            WHERE page_id = p_page_id 
            AND snapshot_date >= DATE_TRUNC('month', p_month)
            AND snapshot_date < DATE_TRUNC('month', p_month) + INTERVAL '1 month'
            ORDER BY snapshot_date DESC LIMIT 1
        ), 0)::INTEGER as current_followers,
        COALESCE((
            SELECT followers_count FROM fb_follower_history 
            WHERE page_id = p_page_id 
            AND snapshot_date >= DATE_TRUNC('month', p_month - INTERVAL '1 month')
            AND snapshot_date < DATE_TRUNC('month', p_month)
            ORDER BY snapshot_date DESC LIMIT 1
        ), 0)::INTEGER as previous_followers,
        (COALESCE((
            SELECT followers_count FROM fb_follower_history 
            WHERE page_id = p_page_id 
            AND snapshot_date >= DATE_TRUNC('month', p_month)
            AND snapshot_date < DATE_TRUNC('month', p_month) + INTERVAL '1 month'
            ORDER BY snapshot_date DESC LIMIT 1
        ), 0) - COALESCE((
            SELECT followers_count FROM fb_follower_history 
            WHERE page_id = p_page_id 
            AND snapshot_date >= DATE_TRUNC('month', p_month - INTERVAL '1 month')
            AND snapshot_date < DATE_TRUNC('month', p_month)
            ORDER BY snapshot_date DESC LIMIT 1
        ), 0))::INTEGER as follower_growth,
        CASE 
            WHEN COALESCE((
                SELECT followers_count FROM fb_follower_history 
                WHERE page_id = p_page_id 
                AND snapshot_date >= DATE_TRUNC('month', p_month - INTERVAL '1 month')
                AND snapshot_date < DATE_TRUNC('month', p_month)
                ORDER BY snapshot_date DESC LIMIT 1
            ), 0) > 0 THEN
                ROUND(
                    ((COALESCE((
                        SELECT followers_count FROM fb_follower_history 
                        WHERE page_id = p_page_id 
                        AND snapshot_date >= DATE_TRUNC('month', p_month)
                        AND snapshot_date < DATE_TRUNC('month', p_month) + INTERVAL '1 month'
                        ORDER BY snapshot_date DESC LIMIT 1
                    ), 0) - COALESCE((
                        SELECT followers_count FROM fb_follower_history 
                        WHERE page_id = p_page_id 
                        AND snapshot_date >= DATE_TRUNC('month', p_month - INTERVAL '1 month')
                        AND snapshot_date < DATE_TRUNC('month', p_month)
                        ORDER BY snapshot_date DESC LIMIT 1
                    ), 0))::NUMERIC / COALESCE((
                        SELECT followers_count FROM fb_follower_history 
                        WHERE page_id = p_page_id 
                        AND snapshot_date >= DATE_TRUNC('month', p_month - INTERVAL '1 month')
                        AND snapshot_date < DATE_TRUNC('month', p_month)
                        ORDER BY snapshot_date DESC LIMIT 1
                    ), 1)) * 100
                , 2)
            ELSE 0
        END as growth_percentage;
END;
$$ LANGUAGE plpgsql;
