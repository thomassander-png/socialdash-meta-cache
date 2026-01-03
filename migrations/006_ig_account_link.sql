-- Migration 006: Add linked_fb_page_id to ig_accounts
-- This column links Instagram Business Accounts to their parent Facebook Pages

-- Add linked_fb_page_id column if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'ig_accounts' AND column_name = 'linked_fb_page_id'
    ) THEN
        ALTER TABLE ig_accounts ADD COLUMN linked_fb_page_id TEXT;
    END IF;
END $$;

-- Create index for faster lookups by linked page
CREATE INDEX IF NOT EXISTS idx_ig_accounts_linked_fb_page ON ig_accounts(linked_fb_page_id);
