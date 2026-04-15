-- hyperfun-storage initial schema
-- All 7 tables defined in docs/superpowers/specs/2026-04-14-postgres-storage-design.md

CREATE TABLE IF NOT EXISTS bar_scores (
    id            BIGSERIAL PRIMARY KEY,
    ts            BIGINT NOT NULL,
    symbol        TEXT NOT NULL,
    open_time     BIGINT NOT NULL,
    close_price   DOUBLE PRECISION NOT NULL,
    composite     DOUBLE PRECISION NOT NULL,
    trend         DOUBLE PRECISION,
    momentum      DOUBLE PRECISION,
    volatility    DOUBLE PRECISION,
    hl_native     DOUBLE PRECISION,
    funding       DOUBLE PRECISION,
    action        TEXT NOT NULL CHECK (action IN ('open_long','open_short','close','hold')),
    UNIQUE (symbol, open_time)
);
CREATE INDEX IF NOT EXISTS idx_bar_scores_symbol_open_time ON bar_scores (symbol, open_time);
CREATE INDEX IF NOT EXISTS idx_bar_scores_ts ON bar_scores (ts);

CREATE TABLE IF NOT EXISTS trades (
    id            BIGSERIAL PRIMARY KEY,
    ts            BIGINT NOT NULL,
    symbol        TEXT NOT NULL,
    event         TEXT NOT NULL CHECK (event IN ('open','close')),
    direction     TEXT NOT NULL CHECK (direction IN ('Long','Short','Unknown')),
    price         DOUBLE PRECISION NOT NULL,
    fill_price    DOUBLE PRECISION NOT NULL,
    composite     DOUBLE PRECISION NOT NULL,
    atr           DOUBLE PRECISION NOT NULL,
    stop_loss     DOUBLE PRECISION,
    pnl           DOUBLE PRECISION,
    reason        TEXT CHECK (reason IN ('signal','stop_loss','trailing_stop','direction_flip')),
    UNIQUE (symbol, ts, event)
);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts ON trades (symbol, ts);
CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades (ts);

CREATE TABLE IF NOT EXISTS positions (
    symbol         TEXT PRIMARY KEY,
    direction      TEXT NOT NULL CHECK (direction IN ('Long','Short')),
    size_usd       DOUBLE PRECISION NOT NULL,
    entry_price    DOUBLE PRECISION NOT NULL,
    entry_time     BIGINT NOT NULL,
    stop_loss      DOUBLE PRECISION NOT NULL,
    extreme_price  DOUBLE PRECISION NOT NULL,
    fees_paid      DOUBLE PRECISION NOT NULL,
    funding_paid   DOUBLE PRECISION NOT NULL,
    updated_at     BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS cooldowns (
    symbol              TEXT PRIMARY KEY,
    cooldown_until_ts   BIGINT NOT NULL,
    cooldown_bars       INT NOT NULL,
    updated_at          BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS market_data_snapshots (
    id          BIGSERIAL PRIMARY KEY,
    ts          BIGINT NOT NULL,
    symbol      TEXT,
    data_type   TEXT NOT NULL CHECK (data_type IN ('funding','hlp','whale','oi')),
    payload     JSONB NOT NULL,
    CHECK (data_type = 'oi' OR symbol IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_mds_data_type_ts ON market_data_snapshots (data_type, ts);
CREATE INDEX IF NOT EXISTS idx_mds_ts ON market_data_snapshots (ts);

CREATE TABLE IF NOT EXISTS daily_stats (
    date             DATE NOT NULL,
    symbol           TEXT NOT NULL,
    total_trades     INT NOT NULL,
    winning_trades   INT NOT NULL,
    losing_trades    INT NOT NULL,
    total_pnl        DOUBLE PRECISION NOT NULL,
    gross_profit     DOUBLE PRECISION NOT NULL,
    gross_loss       DOUBLE PRECISION NOT NULL,
    max_drawdown     DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (date, symbol)
);

CREATE TABLE IF NOT EXISTS candle_cache (
    symbol       TEXT NOT NULL,
    interval     TEXT NOT NULL,
    open_time    BIGINT NOT NULL,
    close_time   BIGINT NOT NULL,
    open         DOUBLE PRECISION NOT NULL,
    high         DOUBLE PRECISION NOT NULL,
    low          DOUBLE PRECISION NOT NULL,
    close        DOUBLE PRECISION NOT NULL,
    volume       DOUBLE PRECISION NOT NULL,
    num_trades   BIGINT NOT NULL,
    PRIMARY KEY (symbol, interval, open_time)
);
CREATE INDEX IF NOT EXISTS idx_candle_cache_lookup ON candle_cache (symbol, interval, close_time DESC);
