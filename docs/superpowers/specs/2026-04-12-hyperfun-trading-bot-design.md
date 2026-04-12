# Hyperfun: Hyperliquid Perpetual Futures Automated Trading Bot

## Overview

A trend-following (right-side trading) automated bot for perpetual futures on Hyperliquid. Uses a multi-factor signal engine across multiple symbols and timeframes, with comprehensive risk management, dual execution modes (paper/live), and full observability via Telegram, structured logging, and a web dashboard.

## Architecture: Rust Monolith

Single Rust process housing all core modules, communicating in-process for minimum latency. TypeScript dashboard as a separate service. PostgreSQL for persistence.

```
                    ┌─────────────────────────────────────────────┐
                    │              Rust Monolith                   │
                    │                                             │
 Hyperliquid ─WS──>│  MarketDataEngine                           │
                    │    │  (multi-symbol candle aggregation,     │
                    │    │   orderbook, trades)                   │
                    │    v                                        │
                    │  SignalEngine                                │
                    │    │  (multi-timeframe x multi-symbol       │
                    │    │   x multi-factor)                      │
                    │    v                                        │
                    │  RiskManager ──> Approve / Reject           │
                    │    │                                        │
                    │    v                                        │
                    │  ExecutionEngine                             │
                    │    │  (Paper Mode / Live Mode)               │
                    │    v                                        │
                    │  PortfolioTracker                            │
                    │    │  (positions, PnL, equity curve)        │
                    │    v                                        │
                    │  NotificationService ──> Telegram            │
                    │    │                                        │
                    │    v                                        │
                    │  REST API Server ──> TS Dashboard            │
                    └─────────────────────────────────────────────┘
                              │
                              v
                    ┌─────────────────┐    ┌──────────────┐
                    │   Config (TOML)  │    │  PostgreSQL   │
                    └─────────────────┘    └──────────────┘
```

**Priority ordering**: Performance > Stability > Flexibility > Observability

## Module 1: MarketDataEngine

### Data Sources

- **WebSocket (primary)**: Real-time trades, L2 orderbook, candles from Hyperliquid
- **REST API (supplementary)**: Historical candle backfill, funding rate, open interest

### Design

- **Candle aggregation from trades**: Not relying on exchange-pushed candles. Custom aggregation enables arbitrary timeframes. On startup, backfill history via REST, then aggregate in real-time.
- **Multi-symbol concurrency**: Single WebSocket connection subscribes to all symbols. Incoming data dispatched by symbol to independent `CandleAggregator` tasks via `tokio`.
- **Storage**: Ring buffer per symbol per timeframe, holding last N candles (configurable, default 500). In-memory only; backfilled from REST on restart.
- **Funding/OI polling**: REST poll every 30s, storing recent history.
- **Fault tolerance**:
  - WebSocket auto-reconnect with exponential backoff
  - Gap detection on reconnect, REST backfill for missed candles
  - Anomaly detection: price jumps beyond threshold flagged to avoid false signals

## Module 2: SignalEngine

### Multi-Factor Architecture

```
CandleStore --> IndicatorLayer --> FactorScorer --> SignalAggregator --> TradeSignal
                 (raw values)     ([-1, +1])       (weighted sum)
```

### Five Factor Groups

| Group | Indicators | Default Weight |
|-------|-----------|----------------|
| Trend | EMA20/50, MACD, Supertrend | 30% |
| Momentum | RSI, CCI | 20% |
| Volatility | ATR, Bollinger Bands | 15% |
| Funding | Volume, Funding Rate, OI | 20% |
| Multi-Timeframe | Higher TF trend alignment | 15% |

### Scoring

- Each indicator outputs [-1.0, +1.0] (-1 = strong bearish, +1 = strong bullish)
- Factor group score = weighted average of indicators within the group
- Final score = weighted sum of all factor groups
- Signal trigger: score > +threshold (default 0.6) = long, < -threshold = short, |score| < exit_threshold (default 0.2) = close

### Multi-Timeframe Coordination

- Higher timeframe (e.g. 4h) acts as a **direction filter**
- Lower timeframe (e.g. 15m) provides **precise entry points**
- Only signals aligned with higher TF direction are accepted
- Timeframe pairs are configurable

### Indicator Trait

```rust
trait Indicator: Send + Sync {
    fn name(&self) -> &str;
    fn update(&mut self, candle: &Candle);
    fn value(&self) -> f64;        // raw indicator value
    fn score(&self) -> f64;        // normalized [-1, +1]
    fn ready(&self) -> bool;       // enough data accumulated
}
```

Adding a new indicator = implement one struct + register it in a factor group.

## Module 3: RiskManager

### Pre-Trade Checks (before order placement)

| Rule | Default | Description |
|------|---------|-------------|
| `max_position_pct` | 5% | Single position max % of total capital |
| `max_concurrent_positions` | 5 | Max simultaneous open positions |
| `max_same_direction` | 3 | Max positions in same direction |
| `max_daily_loss_pct` | 3% | Daily loss limit, pause new positions |
| `max_drawdown_pct` | 10% | Total drawdown limit, close all + halt |
| `min_signal_strength` | 0.6 | Minimum signal score to open |
| `cooldown_after_loss` | 300s | No new position on same symbol after stop loss |

### Post-Trade Monitors (while position is open)

| Mechanism | Description |
|-----------|-------------|
| Fixed stop loss | Initial stop based on ATR at entry |
| Trailing stop | Activates after profit reaches N x ATR, follows price |
| Trailing take profit | Configurable fixed ratio or ATR-multiple partial TP |
| Timeout close | Close if held > N hours with profit below threshold |
| Global circuit breaker | Drawdown threshold -> close all -> Telegram alert -> halt engine |

### Risk Decision Structure

```rust
enum RiskDecision {
    Approved {
        adjusted_size: f64,     // risk may reduce size
        stop_loss: f64,
        take_profit: Option<f64>,
    },
    Rejected {
        reason: RiskRejectReason,
    },
}

enum RiskRejectReason {
    MaxPositionsReached,
    MaxDirectionExposure,
    DailyLossLimitHit,
    DrawdownBreached,
    SignalTooWeak,
    CooldownActive,
}
```

Every rejection is logged with reason for post-analysis.

## Module 4: ExecutionEngine

### Dual Mode via Trait

```rust
trait OrderExecutor: Send + Sync {
    async fn place_order(&self, order: &Order) -> Result<OrderResult>;
    async fn cancel_order(&self, order_id: &str) -> Result<()>;
    async fn get_position(&self, symbol: &str) -> Result<Position>;
    async fn get_balance(&self) -> Result<Balance>;
}
```

Two implementations: `PaperExecutor` and `LiveExecutor`, switched via config.

### PaperExecutor

- Fills at latest trade price (not signal price)
- Configurable simulated slippage (default 0.05%) and fees (Hyperliquid taker rate)
- Simulates funding rate deductions every 8h
- Same trade log format as LiveExecutor for comparison

### LiveExecutor

- Entry: limit order (signal price +/- slippage tolerance) -> timeout -> cancel and reprice or abandon (configurable)
- Stop loss: Stop Market order placed on exchange **immediately** after entry fill (not dependent on local monitoring)
- Take profit: limit order
- Order status synced via WebSocket in real-time

### State Reconciliation

Every 60s:
- Compare exchange positions vs local state
- Mismatch -> trust exchange -> update local -> Telegram alert -> log anomaly

## Module 5: PortfolioTracker

### Components

- **PositionManager**: Current open positions with all metadata
- **PnLCalculator**: Real-time per-position and account-level PnL (realized / unrealized separated)
- **EquityCurve**: Per-minute balance + unrealized PnL snapshots, real-time drawdown calculation, triggers RiskManager on threshold
- **TradeJournal**: Full lifecycle of every trade (entry signal, factor scores, risk decision, fill price, close reason) persisted to PostgreSQL

## Module 6: NotificationService

### Three Channels

| Channel | Purpose |
|---------|---------|
| Telegram Bot | Real-time alerts (open/close/warning/critical) |
| Structured logs | JSON via `tracing`, daily rotation, stdout + file |
| REST API + WebSocket | Real-time data feed for Dashboard |

### Telegram Alert Levels

| Level | Trigger |
|-------|---------|
| INFO | Position open/close |
| WARN | Stop loss hit, daily loss approaching limit |
| CRITICAL | Circuit breaker, WebSocket disconnected, state mismatch |

## Module 7: REST API

| Endpoint | Description |
|----------|-------------|
| `GET /api/status` | System status, uptime, mode |
| `GET /api/positions` | Current positions |
| `GET /api/pnl` | Account PnL, equity curve data |
| `GET /api/signals` | Recent signals |
| `GET /api/trades` | Historical trades (paginated) |
| `GET /api/config` | Current configuration |
| `PUT /api/config` | Hot-reload configuration (subset) |
| `WebSocket /ws/live` | Real-time position/PnL/signal push |

## Module 8: Configuration

### TOML Config File

All parameters configurable: symbol watchlist, timeframe pairs, indicator parameters, factor weights, signal thresholds, risk limits, execution behavior, notification settings.

### Hot Reload

- File watcher via `notify` crate
- Hot-reloadable: symbol watchlist, indicator params, risk params, signal thresholds
- Requires restart: trading mode (paper/live), database connection, WebSocket URL
- Every config change recorded to PostgreSQL with timestamp and diff

### Secrets Management

All secrets via environment variables (`.env` file), never in config or code:
- `HYPERLIQUID_PRIVATE_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `DB_PASSWORD`

`.env` in `.gitignore`, `.env.example` provided as template.

## Module 9: Docker Deployment

```yaml
services:
  bot:
    build: .
    volumes:
      - ./config:/config
      - bot-data:/data
    env_file: .env
    depends_on:
      db:
        condition: service_healthy
    restart: unless-stopped

  db:
    image: postgres:16-alpine
    volumes:
      - pg-data:/var/lib/postgresql/data
    environment:
      POSTGRES_DB: hyperfun
      POSTGRES_USER: bot
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U bot"]
      interval: 5s
      timeout: 3s
      retries: 5
    restart: unless-stopped

  dashboard:
    build: ./dashboard
    ports:
      - "3000:3000"
    environment:
      - API_URL=http://bot:8080
      - DATABASE_URL=postgresql://bot:${DB_PASSWORD}@db:5432/hyperfun
    depends_on:
      - bot
      - db
    restart: unless-stopped

volumes:
  pg-data:
  bot-data:
```

## Project Structure: Rust Workspace

```
hyperfun/
├── Cargo.toml                    # workspace root
├── config/
│   └── default.toml
├── .env.example
├── docker-compose.yml
├── Dockerfile
├── crates/
│   ├── hyperfun-core/            # shared types + traits
│   ├── hyperfun-market/          # market data engine
│   ├── hyperfun-signal/          # signal engine + indicators
│   ├── hyperfun-risk/            # risk management
│   ├── hyperfun-execution/       # paper + live executors
│   ├── hyperfun-portfolio/       # position tracking, PnL, journal
│   ├── hyperfun-notify/          # telegram + logging
│   └── hyperfun-api/             # REST API + WebSocket server
├── src/
│   └── main.rs                   # entry point, wires all modules
└── dashboard/                    # TypeScript frontend
    ├── package.json
    └── src/
```

### Dependency Direction

All crates depend only on `hyperfun-core`. No cross-dependencies between sibling crates. `main.rs` is the sole composition root.

### Key Rust Dependencies

| Purpose | Crate |
|---------|-------|
| Async runtime | `tokio` |
| WebSocket | `tokio-tungstenite` |
| HTTP client | `reqwest` |
| HTTP server | `axum` |
| Serialization | `serde` + `serde_json` |
| Configuration | `config` |
| Database | `sqlx` (PostgreSQL) |
| Logging | `tracing` + `tracing-subscriber` |
| Telegram | `teloxide` |
| File watcher | `notify` |

## V1 Scope

**In scope**:
- All 9 modules above
- Paper trading + live trading (config switch)
- Multi-symbol scanning with configurable watchlist
- Multi-timeframe signal generation
- Full risk management suite
- Telegram + logs + Dashboard + REST API

**Out of scope (V2+)**:
- Backtesting engine
- Python sidecar for advanced strategies
- Multi-account support
- Web-based config editor
