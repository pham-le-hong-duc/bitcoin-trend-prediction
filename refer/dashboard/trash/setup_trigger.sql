-- ============================================================================
-- Setup PostgreSQL NOTIFY triggers for ALL TimescaleDB tables
-- Run this SQL script in your TimescaleDB database
-- ============================================================================

-- ============================================================================
-- PART 1: CREATE TRIGGER FUNCTIONS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Function: notify_aggregated_table (for 5m, 15m, 1h, 4h, 1d intervals)
-- Used for: spot_trades, perpetual_trades, indexpriceklines, 
--           perpetual_markpriceklines, perpetual_orderbook
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION notify_aggregated_table()
RETURNS TRIGGER AS $$
DECLARE
    interval_name TEXT;
    parent_table TEXT;
    table_prefix TEXT;
    channel_name TEXT;
    interval_ms BIGINT;
BEGIN
    -- Get the parent table name (not the chunk)
    SELECT i.relname INTO parent_table
    FROM pg_inherits
    JOIN pg_class c ON (inhrelid = c.oid)
    JOIN pg_class i ON (inhparent = i.oid)
    WHERE c.relname = TG_TABLE_NAME
    LIMIT 1;
    
    -- If parent table found, use it; otherwise use TG_TABLE_NAME
    IF parent_table IS NOT NULL THEN
        -- Extract table prefix and interval: 'perpetual_trades_5m' -> 'perpetual_trades', '5m'
        table_prefix := SUBSTRING(parent_table FROM '^(.+)_([^_]+)$' FOR '#');
        interval_name := SUBSTRING(parent_table FROM '([^_]+)$');
    ELSE
        table_prefix := SUBSTRING(TG_TABLE_NAME FROM '^(.+)_([^_]+)$' FOR '#');
        interval_name := SUBSTRING(TG_TABLE_NAME FROM '([^_]+)$');
    END IF;
    
    -- Only send notification if interval_name is valid (5m, 15m, 1h, 4h, 1d)
    -- AND ts_ms is divisible by interval (correct step size)
    IF interval_name IN ('5m', '15m', '1h', '4h', '1d') THEN
        -- Calculate interval in milliseconds
        interval_ms := CASE interval_name
            WHEN '5m' THEN 5 * 60 * 1000
            WHEN '15m' THEN 15 * 60 * 1000
            WHEN '1h' THEN 60 * 60 * 1000
            WHEN '4h' THEN 4 * 60 * 60 * 1000
            WHEN '1d' THEN 24 * 60 * 60 * 1000
        END;
        
        -- Only notify if ts_ms is at correct interval boundary
        IF NEW.ts_ms % interval_ms = 0 THEN
            -- Build channel name based on parent table
            -- Example: 'spot_trades_5m' -> 'spot_trades_5m'
            channel_name := COALESCE(parent_table, TG_TABLE_NAME);
            
            PERFORM pg_notify(channel_name, interval_name);
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ----------------------------------------------------------------------------
-- Function: notify_fundingrate (special for funding rate - 8h interval)
-- Used for: perpetual_fundingrate
-- Key column: funding_time (instead of ts_ms)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION notify_fundingrate()
RETURNS TRIGGER AS $$
DECLARE
    interval_ms BIGINT;
BEGIN
    -- Funding rate interval is 8 hours
    interval_ms := 8 * 60 * 60 * 1000;
    
    -- Only notify if funding_time is at correct 8h boundary
    IF NEW.funding_time % interval_ms = 0 THEN
        PERFORM pg_notify('perpetual_fundingrate', '8h');
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- PART 2: DROP EXISTING TRIGGERS
-- ============================================================================

-- Spot trades
DROP TRIGGER IF EXISTS notify_spot_trades_5m ON spot_trades_5m;
DROP TRIGGER IF EXISTS notify_spot_trades_15m ON spot_trades_15m;
DROP TRIGGER IF EXISTS notify_spot_trades_1h ON spot_trades_1h;
DROP TRIGGER IF EXISTS notify_spot_trades_4h ON spot_trades_4h;
DROP TRIGGER IF EXISTS notify_spot_trades_1d ON spot_trades_1d;

-- Perpetual trades
DROP TRIGGER IF EXISTS notify_perpetual_trades_5m ON perpetual_trades_5m;
DROP TRIGGER IF EXISTS notify_perpetual_trades_15m ON perpetual_trades_15m;
DROP TRIGGER IF EXISTS notify_perpetual_trades_1h ON perpetual_trades_1h;
DROP TRIGGER IF EXISTS notify_perpetual_trades_4h ON perpetual_trades_4h;
DROP TRIGGER IF EXISTS notify_perpetual_trades_1d ON perpetual_trades_1d;

-- Index price klines
DROP TRIGGER IF EXISTS notify_indexpriceklines_5m ON indexpriceklines_5m;
DROP TRIGGER IF EXISTS notify_indexpriceklines_15m ON indexpriceklines_15m;
DROP TRIGGER IF EXISTS notify_indexpriceklines_1h ON indexpriceklines_1h;
DROP TRIGGER IF EXISTS notify_indexpriceklines_4h ON indexpriceklines_4h;
DROP TRIGGER IF EXISTS notify_indexpriceklines_1d ON indexpriceklines_1d;

-- Mark price klines
DROP TRIGGER IF EXISTS notify_perpetual_markpriceklines_5m ON perpetual_markpriceklines_5m;
DROP TRIGGER IF EXISTS notify_perpetual_markpriceklines_15m ON perpetual_markpriceklines_15m;
DROP TRIGGER IF EXISTS notify_perpetual_markpriceklines_1h ON perpetual_markpriceklines_1h;
DROP TRIGGER IF EXISTS notify_perpetual_markpriceklines_4h ON perpetual_markpriceklines_4h;
DROP TRIGGER IF EXISTS notify_perpetual_markpriceklines_1d ON perpetual_markpriceklines_1d;

-- Orderbook
DROP TRIGGER IF EXISTS notify_perpetual_orderbook_5m ON perpetual_orderbook_5m;
DROP TRIGGER IF EXISTS notify_perpetual_orderbook_15m ON perpetual_orderbook_15m;
DROP TRIGGER IF EXISTS notify_perpetual_orderbook_1h ON perpetual_orderbook_1h;
DROP TRIGGER IF EXISTS notify_perpetual_orderbook_4h ON perpetual_orderbook_4h;
DROP TRIGGER IF EXISTS notify_perpetual_orderbook_1d ON perpetual_orderbook_1d;

-- Funding rate
DROP TRIGGER IF EXISTS notify_perpetual_fundingrate ON perpetual_fundingrate;


-- ============================================================================
-- PART 3: CREATE TRIGGERS FOR ALL TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Spot Trades
-- ----------------------------------------------------------------------------
CREATE TRIGGER notify_spot_trades_5m
AFTER INSERT ON spot_trades_5m
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_spot_trades_15m
AFTER INSERT ON spot_trades_15m
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_spot_trades_1h
AFTER INSERT ON spot_trades_1h
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_spot_trades_4h
AFTER INSERT ON spot_trades_4h
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_spot_trades_1d
AFTER INSERT ON spot_trades_1d
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();


-- ----------------------------------------------------------------------------
-- Perpetual Trades
-- ----------------------------------------------------------------------------
CREATE TRIGGER notify_perpetual_trades_5m
AFTER INSERT ON perpetual_trades_5m
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_perpetual_trades_15m
AFTER INSERT ON perpetual_trades_15m
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_perpetual_trades_1h
AFTER INSERT ON perpetual_trades_1h
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_perpetual_trades_4h
AFTER INSERT ON perpetual_trades_4h
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_perpetual_trades_1d
AFTER INSERT ON perpetual_trades_1d
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();


-- ----------------------------------------------------------------------------
-- Index Price Klines
-- ----------------------------------------------------------------------------
CREATE TRIGGER notify_indexpriceklines_5m
AFTER INSERT ON indexpriceklines_5m
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_indexpriceklines_15m
AFTER INSERT ON indexpriceklines_15m
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_indexpriceklines_1h
AFTER INSERT ON indexpriceklines_1h
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_indexpriceklines_4h
AFTER INSERT ON indexpriceklines_4h
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_indexpriceklines_1d
AFTER INSERT ON indexpriceklines_1d
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();


-- ----------------------------------------------------------------------------
-- Mark Price Klines
-- ----------------------------------------------------------------------------
CREATE TRIGGER notify_perpetual_markpriceklines_5m
AFTER INSERT ON perpetual_markpriceklines_5m
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_perpetual_markpriceklines_15m
AFTER INSERT ON perpetual_markpriceklines_15m
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_perpetual_markpriceklines_1h
AFTER INSERT ON perpetual_markpriceklines_1h
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_perpetual_markpriceklines_4h
AFTER INSERT ON perpetual_markpriceklines_4h
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_perpetual_markpriceklines_1d
AFTER INSERT ON perpetual_markpriceklines_1d
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();


-- ----------------------------------------------------------------------------
-- Orderbook
-- ----------------------------------------------------------------------------
CREATE TRIGGER notify_perpetual_orderbook_5m
AFTER INSERT ON perpetual_orderbook_5m
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_perpetual_orderbook_15m
AFTER INSERT ON perpetual_orderbook_15m
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_perpetual_orderbook_1h
AFTER INSERT ON perpetual_orderbook_1h
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_perpetual_orderbook_4h
AFTER INSERT ON perpetual_orderbook_4h
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();

CREATE TRIGGER notify_perpetual_orderbook_1d
AFTER INSERT ON perpetual_orderbook_1d
FOR EACH ROW
EXECUTE FUNCTION notify_aggregated_table();


-- ----------------------------------------------------------------------------
-- Funding Rate (special case: 8h interval, uses funding_time)
-- ----------------------------------------------------------------------------
CREATE TRIGGER notify_perpetual_fundingrate
AFTER INSERT ON perpetual_fundingrate
FOR EACH ROW
EXECUTE FUNCTION notify_fundingrate();


-- ============================================================================
-- PART 4: VERIFICATION
-- ============================================================================

-- Check all triggers
SELECT 
    trigger_name, 
    event_object_table, 
    action_statement 
FROM information_schema.triggers 
WHERE trigger_name LIKE 'notify_%'
ORDER BY event_object_table, trigger_name;

-- Summary
SELECT 
    'Total triggers created' as description,
    COUNT(*) as count
FROM information_schema.triggers 
WHERE trigger_name LIKE 'notify_%';
