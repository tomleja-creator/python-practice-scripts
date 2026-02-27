-- Initialization script for PostgreSQL database
-- Creates tables and indexes for the PowerApps warehouse

-- Connect to the database
\connect powerapps_warehouse;

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Opportunities table
CREATE TABLE IF NOT EXISTS opportunities (
    opportunity_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200),
    customer VARCHAR(200),
    product VARCHAR(200),
    amount DECIMAL(15,2),
    probability INTEGER,
    stage VARCHAR(50),
    region VARCHAR(100),
    sales_rep VARCHAR(100),
    created_date TIMESTAMP,
    close_date TIMESTAMP,
    actual_revenue DECIMAL(15,2),
    notes TEXT,
    weighted_amount DECIMAL(15,2),
    days_to_close INTEGER,
    deal_size VARCHAR(50),
    high_value BOOLEAN,
    created_month VARCHAR(7),
    created_year INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer feedback table
CREATE TABLE IF NOT EXISTS customer_feedback (
    feedback_id VARCHAR(50) PRIMARY KEY,
    customer VARCHAR(200),
    feedback_type VARCHAR(50),
    rating INTEGER,
    comment TEXT,
    submitted_date TIMESTAMP,
    responded BOOLEAN,
    response_days INTEGER,
    source VARCHAR(50),
    sentiment VARCHAR(20),
    has_comment BOOLEAN,
    responded_within_2days BOOLEAN,
    submitted_month VARCHAR(7),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inventory table
CREATE TABLE IF NOT EXISTS inventory (
    item_id VARCHAR(50) PRIMARY KEY,
    sku VARCHAR(100),
    product VARCHAR(200),
    category VARCHAR(100),
    quantity INTEGER,
    status VARCHAR(50),
    location VARCHAR(200),
    reorder_point INTEGER,
    unit_cost DECIMAL(15,2),
    unit_price DECIMAL(15,2),
    last_updated TIMESTAMP,
    supplier VARCHAR(200),
    lead_time_days INTEGER,
    inventory_value DECIMAL(15,2),
    potential_revenue DECIMAL(15,2),
    margin DECIMAL(15,2),
    margin_percent DECIMAL(5,2),
    needs_reorder BOOLEAN,
    health_score INTEGER,
    turnover_category VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Load history tracking
CREATE TABLE IF NOT EXISTS load_history (
    id SERIAL PRIMARY KEY,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(500),
    entity VARCHAR(50),
    records_loaded INTEGER,
    status VARCHAR(20)
);

-- Sales summary table
CREATE TABLE IF NOT EXISTS sales_summary (
    id SERIAL PRIMARY KEY,
    report_date VARCHAR(10),
    region VARCHAR(100),
    total_opportunities INTEGER,
    total_amount DECIMAL(15,2),
    weighted_amount DECIMAL(15,2),
    won_amount DECIMAL(15,2),
    lost_amount DECIMAL(15,2),
    win_rate DECIMAL(5,2),
    avg_deal_size DECIMAL(15,2),
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_opportunities_created_date ON opportunities(created_date);
CREATE INDEX IF NOT EXISTS idx_opportunities_stage ON opportunities(stage);
CREATE INDEX IF NOT EXISTS idx_opportunities_region ON opportunities(region);
CREATE INDEX IF NOT EXISTS idx_opportunities_customer ON opportunities(customer);

CREATE INDEX IF NOT EXISTS idx_feedback_submitted_date ON customer_feedback(submitted_date);
CREATE INDEX IF NOT EXISTS idx_feedback_rating ON customer_feedback(rating);
CREATE INDEX IF NOT EXISTS idx_feedback_sentiment ON customer_feedback(sentiment);

CREATE INDEX IF NOT EXISTS idx_inventory_status ON inventory(status);
CREATE INDEX IF NOT EXISTS idx_inventory_category ON inventory(category);
CREATE INDEX IF NOT EXISTS idx_inventory_needs_reorder ON inventory(needs_reorder);

CREATE INDEX IF NOT EXISTS idx_load_history_timestamp ON load_history(load_timestamp);
CREATE INDEX IF NOT EXISTS idx_load_history_entity ON load_history(entity);

-- Create views for common queries
CREATE OR REPLACE VIEW vw_pipeline_summary AS
SELECT 
    date_trunc('month', created_date) as month,
    region,
    COUNT(*) as total_deals,
    SUM(amount) as pipeline_value,
    SUM(weighted_amount) as weighted_value,
    AVG(CASE WHEN stage IN ('Closed Won', 'Closed Lost') 
        THEN 1.0 ELSE NULL END) as close_rate
FROM opportunities
GROUP BY date_trunc('month', created_date), region;

CREATE OR REPLACE VIEW vw_customer_satisfaction AS
SELECT 
    date_trunc('month', submitted_date) as month,
    feedback_type,
    AVG(rating) as avg_rating,
    COUNT(*) as total_feedback,
    SUM(CASE WHEN sentiment = 'Positive' THEN 1 ELSE 0 END) as positive_count,
    AVG(response_days) as avg_response_days
FROM customer_feedback
GROUP BY date_trunc('month', submitted_date), feedback_type;

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO pipeline;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO pipeline;
