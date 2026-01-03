-- Migration 005: Admin Tables for Customer & Account Management
-- Creates tables for customer management, account assignment, and report tracking

-- Enable UUID extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1) Customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- 2) Customer Accounts table (links FB/IG accounts to customers)
CREATE TABLE IF NOT EXISTS customer_accounts (
    id SERIAL PRIMARY KEY,
    customer_id UUID REFERENCES customers(customer_id) ON DELETE SET NULL,
    platform TEXT NOT NULL CHECK (platform IN ('facebook', 'instagram')),
    account_id TEXT NOT NULL,
    account_name TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(platform, account_id)
);

-- 3) Reports table
CREATE TABLE IF NOT EXISTS reports (
    report_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id UUID NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    month DATE NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'generating', 'generated', 'failed')),
    pptx_url TEXT,
    pdf_url TEXT,
    generated_at TIMESTAMPTZ,
    error_message TEXT,
    meta JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(customer_id, month)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_customer_accounts_customer_id ON customer_accounts(customer_id);
CREATE INDEX IF NOT EXISTS idx_customer_accounts_platform ON customer_accounts(platform);
CREATE INDEX IF NOT EXISTS idx_customer_accounts_account_id ON customer_accounts(account_id);
CREATE INDEX IF NOT EXISTS idx_reports_customer_id ON reports(customer_id);
CREATE INDEX IF NOT EXISTS idx_reports_month ON reports(month);
CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status);

-- View: Customer with account counts
CREATE OR REPLACE VIEW view_customer_summary AS
SELECT 
    c.customer_id,
    c.name,
    c.is_active,
    c.created_at,
    COUNT(DISTINCT CASE WHEN ca.platform = 'facebook' THEN ca.id END) as fb_account_count,
    COUNT(DISTINCT CASE WHEN ca.platform = 'instagram' THEN ca.id END) as ig_account_count,
    COUNT(DISTINCT ca.id) as total_account_count
FROM customers c
LEFT JOIN customer_accounts ca ON c.customer_id = ca.customer_id AND ca.is_active = true
GROUP BY c.customer_id, c.name, c.is_active, c.created_at;

-- View: Unassigned accounts
CREATE OR REPLACE VIEW view_unassigned_accounts AS
SELECT 
    ca.id,
    ca.platform,
    ca.account_id,
    ca.account_name,
    ca.is_active,
    ca.created_at
FROM customer_accounts ca
WHERE ca.customer_id IS NULL;

-- View: Customer reports with details
CREATE OR REPLACE VIEW view_customer_reports AS
SELECT 
    r.report_id,
    r.customer_id,
    c.name as customer_name,
    r.month,
    r.status,
    r.pptx_url,
    r.pdf_url,
    r.generated_at,
    r.error_message,
    r.meta,
    r.created_at
FROM reports r
JOIN customers c ON r.customer_id = c.customer_id
ORDER BY r.month DESC, c.name;
