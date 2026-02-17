-- Migration 009: Add report_config column to customers table
-- Stores per-customer report configuration as JSONB

ALTER TABLE customers ADD COLUMN IF NOT EXISTS report_config JSONB DEFAULT '{}';
ALTER TABLE customers ADD COLUMN IF NOT EXISTS logo_url TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS primary_color TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS contact_name TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS contact_email TEXT;

COMMENT ON COLUMN customers.report_config IS 'Per-customer report configuration (slides, KPIs, platforms, reichweite_definition, etc.)';
