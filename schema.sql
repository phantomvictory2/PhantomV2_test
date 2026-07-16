-- Raw signals (every signal fired, including skipped)
CREATE TABLE signals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset VARCHAR(10),
    strategy_type VARCHAR(30),
    direction VARCHAR(10),
    magnitude_pct FLOAT,
    magnitude_usd FLOAT,
    duration_seconds INT,
    grade VARCHAR(10),
    poly_staleness_seconds FLOAT,
    spread FLOAT,
    yes_price FLOAT,
    no_price FLOAT,
    market_id VARCHAR(100),
    time_to_resolution_seconds INT,
    liquidity_usdc FLOAT,
    velocity_count INT,
    outcome VARCHAR(20),        -- EXECUTED | REJECTED | SKIPPED
    skip_reason TEXT,
    rejection_reason VARCHAR(50),
    ttr_at_rejection INT,
    oracle_lag_at_rejection FLOAT,
    signal_velocity_at_rejection INT,
    spread_at_rejection FLOAT,
    fired_at TIMESTAMPTZ DEFAULT NOW()
);

-- All positions (paper and live)
CREATE TABLE positions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    signal_id UUID REFERENCES signals(id),
    strategy_type VARCHAR(30),
    market_id VARCHAR(100),
    asset VARCHAR(10),
    direction VARCHAR(10),
    entry_price FLOAT,
    size_usdc FLOAT,
    entry_mode VARCHAR(10),     -- SINGLE | DCA
    dca_rounds_completed INT,
    status VARCHAR(20),         -- OPEN | CLOSED | STOPPED
    exit_price FLOAT,
    pnl FLOAT,
    close_reason VARCHAR(50),   -- RESOLUTION | TAKE_PROFIT | 
                                -- STOP_LOSS | SAFETY_EXIT | DCA_STOPPED
    actual_lag_seconds FLOAT,   -- real oracle lag captured
    signal_to_fill_ms INT,      -- execution speed tracking
    is_paper BOOLEAN DEFAULT TRUE,
    would_new_safeguard_have_blocked BOOLEAN DEFAULT FALSE,
    safeguard_replay_confidence VARCHAR(20) DEFAULT NULL,
    opened_at TIMESTAMPTZ DEFAULT NOW(),
    closed_at TIMESTAMPTZ
);

-- Daily aggregates
CREATE TABLE daily_stats (
    date DATE PRIMARY KEY,
    trades_count INT DEFAULT 0,
    wins INT DEFAULT 0,
    losses INT DEFAULT 0,
    win_rate FLOAT,
    loss_rate FLOAT,
    gross_pnl FLOAT DEFAULT 0,
    net_pnl FLOAT DEFAULT 0,
    starting_bankroll FLOAT,
    ending_bankroll FLOAT,
    regime_stopped BOOLEAN DEFAULT FALSE,
    loss_limit_hit BOOLEAN DEFAULT FALSE,
    is_paper BOOLEAN DEFAULT TRUE
);

-- Risk engine decisions
CREATE TABLE risk_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(50),     -- SIGNAL_VELOCITY_PAUSE | REGIME_STOP |
                                -- ORACLE_DISABLED | CONSECUTIVE_COOLDOWN |
                                -- DAILY_LIMIT_HIT | KILL_SWITCH
    signal_id UUID,
    asset VARCHAR(10),
    strategy_type VARCHAR(30),
    trigger_value TEXT,
    action_taken TEXT,
    timestamp TIMESTAMPTZ DEFAULT NOW()
);

-- Asset oracle performance (feeds Risk Engine Check 7)
CREATE TABLE asset_lag_stats (
    asset VARCHAR(10) PRIMARY KEY,
    avg_lag_seconds FLOAT,
    sample_size INT DEFAULT 0,
    min_lag_seconds FLOAT,
    max_lag_seconds FLOAT,
    status VARCHAR(20) DEFAULT 'ACTIVE',  -- ACTIVE | DISABLED
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

-- Strategy performance (feeds capital allocation)
CREATE TABLE strategy_stats (
    strategy_type VARCHAR(30) PRIMARY KEY,
    trades_total INT DEFAULT 0,
    wins INT DEFAULT 0,
    losses INT DEFAULT 0,
    win_rate FLOAT DEFAULT 0,
    avg_pnl_per_trade FLOAT DEFAULT 0,
    total_pnl FLOAT DEFAULT 0,
    profit_factor FLOAT DEFAULT 0,
    max_drawdown FLOAT DEFAULT 0,
    avg_execution_ms FLOAT DEFAULT 0,
    capital_weight FLOAT DEFAULT 0.25,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

-- Runtime configuration (adjustable without redeployment)
CREATE TABLE system_config (
    key VARCHAR(50) PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO system_config VALUES
    ('kill_switch', 'false', NOW()),
    ('paper_mode', 'true', NOW()),
    ('daily_loss_limit_pct', '0.05', NOW()),
    ('regime_stop_loss_pct', '0.03', NOW()),
    ('regime_stop_winrate', '0.40', NOW()),
    ('max_open_positions', '3', NOW()),
    ('max_trade_usdc', '100', NOW()),
    ('consecutive_loss_limit', '3', NOW()),
    ('signal_velocity_limit', '5', NOW()),
    ('oracle_lag_minimum_seconds', '5', NOW()),
    ('dca_adverse_price_threshold', '0.10', NOW())
ON CONFLICT (key) DO NOTHING;

-- Phase 0 Edge Verification Logging
CREATE TABLE phase0_edge_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_type VARCHAR(30),
    grade VARCHAR(10),
    asset VARCHAR(10),
    market_id VARCHAR(100),
    direction VARCHAR(10),
    signal_time TIMESTAMPTZ,
    entry_price_would_have_been FLOAT,
    resolution_price FLOAT,          -- filled in once market resolves
    resolution_time TIMESTAMPTZ,
    raw_pnl_per_dollar FLOAT,        -- before fees
    fee_pct_at_entry FLOAT,          -- taker fee at that price point
    fee_adjusted_pnl_per_dollar FLOAT,
    won BOOLEAN,
    logged_at TIMESTAMPTZ DEFAULT NOW()
);

-- DCA Execution Journal for state recovery
CREATE TABLE IF NOT EXISTS dca_execution_journal (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    signal_id UUID NOT NULL REFERENCES signals(id),
    asset VARCHAR(10) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    rounds_total INT NOT NULL,
    rounds_completed INT NOT NULL DEFAULT 0,
    per_round_usdc FLOAT NOT NULL,
    limit_price FLOAT NOT NULL,
    interval_seconds INT NOT NULL,
    total_size_filled FLOAT NOT NULL DEFAULT 0.0,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE | COMPLETED | FAILED | STOPPED
    journal_version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

-- Trade validator rejection log
CREATE TABLE IF NOT EXISTS trade_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    signal_id VARCHAR(100),
    asset VARCHAR(10),
    strategy_type VARCHAR(30),
    direction VARCHAR(10),
    gate_blocked_at VARCHAR(50),
    block_reason TEXT,
    confidence_score INT,
    logged_at TIMESTAMPTZ DEFAULT NOW()
);
