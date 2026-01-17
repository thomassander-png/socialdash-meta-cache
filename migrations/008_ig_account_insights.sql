-- Migration 008: Instagram Account Insights
-- Stores account-level insights from Instagram Graph API

-- Table for daily Instagram account insights snapshots
CREATE TABLE IF NOT EXISTS ig_account_insights (
    id SERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    
    -- Profile link taps (clicks on profile buttons)
    profile_links_taps INTEGER,
    email_clicks INTEGER,
    call_clicks INTEGER,
    direction_clicks INTEGER,
    text_clicks INTEGER,
    
    -- Engagement metrics
    total_interactions INTEGER,
    views INTEGER,  -- replaces impressions (deprecated)
    reach INTEGER,
    accounts_engaged INTEGER,
    
    -- Raw JSON for debugging and future metrics
    raw_json JSONB,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Unique constraint: one snapshot per account per day
    UNIQUE(account_id, snapshot_date)
);

-- Index for efficient queries
CREATE INDEX IF NOT EXISTS idx_ig_account_insights_account_date 
ON ig_account_insights(account_id, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_ig_account_insights_date 
ON ig_account_insights(snapshot_date DESC);

-- View for latest account insights
CREATE OR REPLACE VIEW view_ig_account_latest_insights AS
SELECT DISTINCT ON (account_id)
    iai.*,
    ia.username,
    ia.name as account_name
FROM ig_account_insights iai
LEFT JOIN ig_accounts ia ON iai.account_id = ia.account_id
ORDER BY iai.account_id, iai.snapshot_date DESC;

-- View for monthly aggregated account insights
CREATE OR REPLACE VIEW view_ig_account_monthly_insights AS
SELECT 
    account_id,
    DATE_TRUNC('month', snapshot_date)::DATE as month,
    SUM(profile_links_taps) as total_profile_links_taps,
    SUM(email_clicks) as total_email_clicks,
    SUM(call_clicks) as total_call_clicks,
    SUM(direction_clicks) as total_direction_clicks,
    SUM(text_clicks) as total_text_clicks,
    SUM(total_interactions) as total_interactions,
    SUM(views) as total_views,
    AVG(reach) as avg_reach,
    MAX(accounts_engaged) as max_accounts_engaged,
    COUNT(*) as snapshot_count
FROM ig_account_insights
GROUP BY account_id, DATE_TRUNC('month', snapshot_date)
ORDER BY account_id, month DESC;

-- Add impressions column to fb_post_metrics if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'fb_post_metrics' AND column_name = 'impressions'
    ) THEN
        ALTER TABLE fb_post_metrics ADD COLUMN impressions INTEGER;
    END IF;
END $$;

-- Add video_views column to fb_post_metrics if not exists (alias for video_3s_views)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'fb_post_metrics' AND column_name = 'video_views'
    ) THEN
        ALTER TABLE fb_post_metrics ADD COLUMN video_views INTEGER;
    END IF;
END $$;
