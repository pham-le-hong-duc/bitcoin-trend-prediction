-- ============================================================================
-- DATABASE OPTIMIZATION FOR DASHBOARD
-- Add indexes to speed up queries and JOINs
-- ============================================================================

-- ============================================================================
-- PART 1: CREATE INDEXES ON ts_ms (Primary query column)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Spot Trades (5 intervals)
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_spot_trades_5m_ts_ms ON spot_trades_5m(ts_ms);
CREATE INDEX IF NOT EXISTS idx_spot_trades_15m_ts_ms ON spot_trades_15m(ts_ms);
CREATE INDEX IF NOT EXISTS idx_spot_trades_1h_ts_ms ON spot_trades_1h(ts_ms);
CREATE INDEX IF NOT EXISTS idx_spot_trades_4h_ts_ms ON spot_trades_4h(ts_ms);
CREATE INDEX IF NOT EXISTS idx_spot_trades_1d_ts_ms ON spot_trades_1d(ts_ms);

-- ----------------------------------------------------------------------------
-- Perpetual Trades (5 intervals)
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_perpetual_trades_5m_ts_ms ON perpetual_trades_5m(ts_ms);
CREATE INDEX IF NOT EXISTS idx_perpetual_trades_15m_ts_ms ON perpetual_trades_15m(ts_ms);
CREATE INDEX IF NOT EXISTS idx_perpetual_trades_1h_ts_ms ON perpetual_trades_1h(ts_ms);
CREATE INDEX IF NOT EXISTS idx_perpetual_trades_4h_ts_ms ON perpetual_trades_4h(ts_ms);
CREATE INDEX IF NOT EXISTS idx_perpetual_trades_1d_ts_ms ON perpetual_trades_1d(ts_ms);

-- ----------------------------------------------------------------------------
-- Index Price Klines (5 intervals)
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_indexpriceklines_5m_ts_ms ON indexpriceklines_5m(ts_ms);
CREATE INDEX IF NOT EXISTS idx_indexpriceklines_15m_ts_ms ON indexpriceklines_15m(ts_ms);
CREATE INDEX IF NOT EXISTS idx_indexpriceklines_1h_ts_ms ON indexpriceklines_1h(ts_ms);
CREATE INDEX IF NOT EXISTS idx_indexpriceklines_4h_ts_ms ON indexpriceklines_4h(ts_ms);
CREATE INDEX IF NOT EXISTS idx_indexpriceklines_1d_ts_ms ON indexpriceklines_1d(ts_ms);

-- ----------------------------------------------------------------------------
-- Mark Price Klines (5 intervals)
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_perpetual_markpriceklines_5m_ts_ms ON perpetual_markpriceklines_5m(ts_ms);
CREATE INDEX IF NOT EXISTS idx_perpetual_markpriceklines_15m_ts_ms ON perpetual_markpriceklines_15m(ts_ms);
CREATE INDEX IF NOT EXISTS idx_perpetual_markpriceklines_1h_ts_ms ON perpetual_markpriceklines_1h(ts_ms);
CREATE INDEX IF NOT EXISTS idx_perpetual_markpriceklines_4h_ts_ms ON perpetual_markpriceklines_4h(ts_ms);
CREATE INDEX IF NOT EXISTS idx_perpetual_markpriceklines_1d_ts_ms ON perpetual_markpriceklines_1d(ts_ms);

-- ----------------------------------------------------------------------------
-- Orderbook (5 intervals)
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_perpetual_orderbook_5m_ts_ms ON perpetual_orderbook_5m(ts_ms);
CREATE INDEX IF NOT EXISTS idx_perpetual_orderbook_15m_ts_ms ON perpetual_orderbook_15m(ts_ms);
CREATE INDEX IF NOT EXISTS idx_perpetual_orderbook_1h_ts_ms ON perpetual_orderbook_1h(ts_ms);
CREATE INDEX IF NOT EXISTS idx_perpetual_orderbook_4h_ts_ms ON perpetual_orderbook_4h(ts_ms);
CREATE INDEX IF NOT EXISTS idx_perpetual_orderbook_1d_ts_ms ON perpetual_orderbook_1d(ts_ms);

-- ----------------------------------------------------------------------------
-- Funding Rate (special: uses funding_time, 8h interval)
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_perpetual_fundingrate_funding_time ON perpetual_fundingrate(funding_time);


-- ============================================================================
-- PART 2: ANALYZE TABLES TO UPDATE STATISTICS
-- ============================================================================

-- Spot Trades
ANALYZE spot_trades_5m;
ANALYZE spot_trades_15m;
ANALYZE spot_trades_1h;
ANALYZE spot_trades_4h;
ANALYZE spot_trades_1d;

-- Perpetual Trades
ANALYZE perpetual_trades_5m;
ANALYZE perpetual_trades_15m;
ANALYZE perpetual_trades_1h;
ANALYZE perpetual_trades_4h;
ANALYZE perpetual_trades_1d;

-- Index Price Klines
ANALYZE indexpriceklines_5m;
ANALYZE indexpriceklines_15m;
ANALYZE indexpriceklines_1h;
ANALYZE indexpriceklines_4h;
ANALYZE indexpriceklines_1d;

-- Mark Price Klines
ANALYZE perpetual_markpriceklines_5m;
ANALYZE perpetual_markpriceklines_15m;
ANALYZE perpetual_markpriceklines_1h;
ANALYZE perpetual_markpriceklines_4h;
ANALYZE perpetual_markpriceklines_1d;

-- Orderbook
ANALYZE perpetual_orderbook_5m;
ANALYZE perpetual_orderbook_15m;
ANALYZE perpetual_orderbook_1h;
ANALYZE perpetual_orderbook_4h;
ANALYZE perpetual_orderbook_1d;

-- Funding Rate
ANALYZE perpetual_fundingrate;


-- ============================================================================
-- PART 3: VACUUM ANALYZE (Clean up and update all statistics)
-- ============================================================================
VACUUM ANALYZE;


-- ============================================================================
-- PART 4: CHECK CREATED INDEXES
-- ============================================================================
SELECT 
    schemaname,
    tablename, 
    indexname, 
    indexdef 
FROM pg_indexes 
WHERE schemaname = 'public'
  AND (
    tablename LIKE '%trades%' 
    OR tablename LIKE '%klines%' 
    OR tablename LIKE '%orderbook%'
    OR tablename LIKE '%fundingrate%'
  )
ORDER BY tablename, indexname;


-- ============================================================================
-- PART 5: CHECK TABLE SIZES
-- ============================================================================
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) AS indexes_size
FROM pg_tables
WHERE schemaname = 'public'
  AND (
    tablename LIKE '%trades%' 
    OR tablename LIKE '%klines%' 
    OR tablename LIKE '%orderbook%'
    OR tablename LIKE '%fundingrate%'
  )
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;


-- ============================================================================
-- SUMMARY
-- ============================================================================
-- Total indexes created: 
--   - Spot trades:        5 indexes (ts_ms only)
--   - Perpetual trades:   5 indexes (ts_ms only)
--   - Index klines:       5 indexes (ts_ms only)
--   - Mark klines:        5 indexes (ts_ms only)
--   - Orderbook:          5 indexes (ts_ms only)
--   - Funding rate:       1 index (funding_time)
--   TOTAL:                26 indexes
--
-- Note: timestamp_dt indexes NOT created because ts_ms and timestamp_dt
--       have 1-1 mapping. Index on ts_ms is sufficient for both columns.
--
-- Expected improvements:
--   - JOIN queries: 10-50x faster (from 12s to ~1-2s)
--   - Time range queries: 5-20x faster (via ts_ms conversion)
--   - ORDER BY ts_ms: 10x faster
--   - Less storage overhead compared to dual indexes
--   - Faster INSERTs (only 1 index to update instead of 2)
-- ============================================================================
