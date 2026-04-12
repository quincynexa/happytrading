<!-- /autoplan restore point: /Users/quincy/.gstack/projects/quincynexa-happytrading/main-autoplan-restore-20260412-213538.md -->
# Hyperfun: Hyperliquid Perpetual Futures Trading Bot — Lean MVP

## Overview

Signal validation tool for trend-following (right-side) perpetual futures trading on Hyperliquid. V1 is a **lean MVP**: multi-factor signal engine with Hyperliquid-native data sources + paper executor. Console/log output only. The goal is to prove the strategy has positive expectancy before building infrastructure.

No dashboard. No Telegram. No PostgreSQL. No Docker. No live trading.

## Architecture

Single Rust process, 4 crates. Local execution via `cargo run`.

```
Hyperliquid ─WS──> MarketDataEngine
                      │
                      ├── Candle stream (from HL candle WS)
                      ├── Funding/OI (REST poll)
                      ├── HLP vault positions (REST poll)
                      ├── Liquidation events (WS)
                      └── Whale address positions (REST poll)
                              │
                              v
                      SignalEngine
                        │  (generic TA + HL-native factors)
                        │  (multi-timeframe x multi-symbol)
                        v
                      PaperExecutor
                        │  (realistic fill simulation)
                        v
                      Console + tracing logs
```

**Priority ordering**: Signal Quality > Stability > Flexibility > Observability

## Module 1: MarketDataEngine

### Data Sources

- **WebSocket**: HL candle subscriptions (15m, 1h, 4h), trades, liquidation events
- **REST API**: Historical candle backfill, funding rate, open interest, HLP vault state, whale positions

### Design

- **Use Hyperliquid candle WebSocket** for standard timeframes (15m, 1h, 4h). Do NOT aggregate candles from trades in V1 (hidden complexity: boundary alignment, gap handling, partial candles on disconnect). Custom aggregation deferred to V2 if arbitrary timeframes are needed.
- **Multi-symbol concurrency**: Single WS connection subscribes to all symbols. Data dispatched by symbol to independent tasks via `tokio`.
- **Storage**: Ring buffer per symbol per timeframe, holding last N candles (configurable, default 500). In-memory only; backfilled from REST on restart.
- **HL-native data polling**:
  - HLP vault positions: REST poll every 30s (`clearinghouseState` for HLP address)
  - Funding rate: REST poll every 60s (`predictedFundings`, `fundingHistory`)
  - Open interest: REST poll every 30s (from meta endpoint)
  - Whale positions: REST poll every 60s (configurable address watchlist)
- **Liquidation stream**: Subscribe to liquidation events via WS `userEvents` or parse from trade stream
- **Fault tolerance**:
  - WebSocket auto-reconnect with exponential backoff
  - On reconnect: discard current in-progress candle, backfill completed candles from REST
  - Mark all data as `stale` until backfill completes; signal engine checks stale flag
  - Anomaly detection: price jumps beyond threshold flagged to avoid false signals
  - **Minimum volume filter**: symbols with < configurable daily volume threshold excluded from scanning

### Data Types

```rust
/// Unified market data that flows into the signal engine
enum MarketData {
    Candle(CandleData),           // standard OHLCV
    FundingRate(FundingData),     // current + predicted
    OpenInterest(OIData),         // per-symbol OI
    HlpPosition(HlpData),        // HLP vault inventory
    Liquidation(LiquidationData), // per-event
    WhalePosition(WhaleData),     // tracked address positions
}
```

## Module 2: SignalEngine

### Multi-Factor Architecture

```
MarketData stream --> FactorGroups --> FactorScorer --> SignalAggregator --> TradeSignal
                      (per type)      ([-1, +1])       (weighted sum)
```

### Six Factor Groups

| Group | Indicators | Input Type | Default Weight |
|-------|-----------|------------|----------------|
| Trend | EMA20/50, MACD, Supertrend | Candle | 25% |
| Momentum | RSI, CCI | Candle | 15% |
| Volatility | ATR, Bollinger Bands | Candle | 10% |
| HL-Native | HLP inventory skew, liquidation cascade, whale flow | MarketData (non-candle) | 25% |
| Funding | Predicted funding rate, funding rate trend, OI change | MarketData (non-candle) | 15% |
| Multi-Timeframe | Higher TF trend alignment | Candle (higher TF) | 10% |

### Scoring

- Each indicator outputs [-1.0, +1.0] (-1 = strong bearish, +1 = strong bullish)
- **A factor group only scores when ALL its indicators report `ready() = true`**. If any indicator in a group is not ready, the group score is excluded and weights are NOT renormalized. The signal is suppressed until all groups are ready (warmup period).
- Final score = weighted sum of all ready factor groups
- **Signals generated only on bar close** (when a candle completes), never mid-bar
- Signal trigger: score > +threshold (default 0.6) = long, < -threshold = short, |score| < exit_threshold (default 0.2) = close
- **Idempotency**: one position per symbol max. Repeated same-direction signals are suppressed. Direction flip = close existing + open new.

### Multi-Timeframe Coordination

- Higher timeframe (e.g. 4h) acts as a **direction filter**
- Lower timeframe (e.g. 15m) provides **precise entry points**
- **Always use the last fully closed higher-TF candle** for filtering, never in-progress
- Only signals aligned with higher TF direction are accepted
- Timeframe pairs are configurable

### Indicator Traits

```rust
/// For candle-based TA indicators (EMA, RSI, MACD, etc.)
trait CandleIndicator: Send + Sync {
    fn name(&self) -> &str;
    fn update(&mut self, candle: &Candle);
    fn value(&self) -> f64;
    fn score(&self) -> f64;        // normalized [-1, +1]
    fn ready(&self) -> bool;
}

/// For Hyperliquid-native signal providers (HLP, liquidations, whales)
trait HlSignalProvider: Send + Sync {
    fn name(&self) -> &str;
    fn update(&mut self, data: &MarketData);
    fn score(&self) -> f64;        // normalized [-1, +1]
    fn ready(&self) -> bool;
}
```

Two separate traits: `CandleIndicator` for standard TA, `HlSignalProvider` for HL-native data that doesn't fit the candle model.

## Module 3: PaperExecutor

Paper-only in V1. This is the sole validation tool, so it must be realistic.

### Fill Model

- Fill at latest trade price + **half spread** (estimated from L2 book snapshot)
- Simulated slippage: configurable (default 0.05%)
- Simulated fee: Hyperliquid taker rate (0.035%)
- **Mark price** (not last price) for unrealized PnL calculation
- Funding rate deduction every hour (1/8 of 8h rate, matching HL mechanics)
- Respect min-notional and precision rules per symbol
- **No private key required or loaded in paper mode**

### Position Tracking

- One position per symbol max (matching signal idempotency)
- Track: entry price, size, direction, entry time, unrealized PnL, realized PnL
- Simple stop loss: fixed ATR-based (configurable multiplier)
- No trailing stop in V1 (keep it simple for validation)

### Output

All output to `tracing` structured logs (JSON to stdout):
- Every signal generated (with all factor scores)
- Every paper trade (open/close with PnL)
- Running statistics: win rate, profit factor, total PnL, max drawdown
- Periodic summary every N minutes (configurable)

## Module 4: Configuration

### TOML Config File

```toml
[general]
mode = "paper"                  # only "paper" in V1
log_level = "info"

[symbols]
watchlist = ["BTC", "ETH", "SOL"]
min_daily_volume = 1000000      # minimum daily volume in USD to trade
# scan_all = false              # V2: scan all HL perps

[timeframes]
trend = "4h"                    # higher TF for direction filter
entry = "15m"                   # lower TF for entry signals

[indicators.trend]
ema_short = 20
ema_long = 50
macd_fast = 12
macd_slow = 26
macd_signal = 9
supertrend_period = 10
supertrend_multiplier = 3.0
weight = 0.25

[indicators.momentum]
rsi_period = 14
rsi_overbought = 70
rsi_oversold = 30
cci_period = 20
weight = 0.15

[indicators.volatility]
atr_period = 14
bollinger_period = 20
bollinger_std = 2.0
weight = 0.10

[indicators.hl_native]
hlp_vault_address = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303"
whale_addresses = []            # manually curated list
liquidation_lookback_secs = 300
weight = 0.25

[indicators.funding]
funding_extreme_threshold = 0.01
weight = 0.15

[indicators.mtf]
weight = 0.10

[signal]
open_threshold = 0.6
close_threshold = 0.2

[paper]
simulated_slippage_pct = 0.05
simulated_fee_pct = 0.035
position_size_usd = 1000       # fixed size per trade for paper
atr_stop_multiplier = 2.0
summary_interval_mins = 60

[hyperliquid]
ws_url = "wss://api.hyperliquid.xyz/ws"
rest_url = "https://api.hyperliquid.xyz"
```

### No Hot Reload in V1

Restart the process to apply config changes. Hot reload adds complexity (invalidates warmup windows, factor scores, position state) and is not needed for a validation tool.

### Secrets

Paper mode requires NO secrets. `.env` with `HYPERLIQUID_PRIVATE_KEY` only needed for V2 live mode.

## Project Structure

```
hyperfun/
├── Cargo.toml                    # workspace root
├── config/
│   └── default.toml
├── crates/
│   ├── hyperfun-core/            # shared types, traits, config
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── types.rs          # Candle, MarketData, Signal, Order, Position
│   │       ├── traits.rs         # CandleIndicator, HlSignalProvider
│   │       └── config.rs         # config structs (serde)
│   │
│   ├── hyperfun-market/          # HL data engine
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── ws.rs             # WebSocket connection + reconnect
│   │       ├── rest.rs           # REST client (backfill, funding, HLP, whales)
│   │       ├── candle_store.rs   # ring buffer per symbol per TF
│   │       └── hl_data.rs        # HLP vault, liquidations, whale tracking
│   │
│   ├── hyperfun-signal/          # signal engine
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── indicators/
│   │       │   ├── mod.rs
│   │       │   ├── ema.rs
│   │       │   ├── macd.rs
│   │       │   ├── supertrend.rs
│   │       │   ├── rsi.rs
│   │       │   ├── cci.rs
│   │       │   ├── atr.rs
│   │       │   └── bollinger.rs
│   │       ├── hl_signals/
│   │       │   ├── mod.rs
│   │       │   ├── hlp_inventory.rs
│   │       │   ├── liquidation.rs
│   │       │   ├── whale_flow.rs
│   │       │   └── funding.rs
│   │       ├── factors.rs        # factor group scoring
│   │       └── aggregator.rs     # weighted sum -> TradeSignal
│   │
│   └── hyperfun-executor/        # paper executor
│       └── src/
│           ├── lib.rs
│           ├── paper.rs          # paper fill model
│           ├── position.rs       # position tracking + PnL
│           └── stats.rs          # running statistics
│
└── src/
    └── main.rs                   # entry point, wires all crates
```

### Dependency Direction

```
hyperfun-core       (shared types + traits, no internal deps)
    ^
    |
hyperfun-market     (depends on core)
    ^
    |
hyperfun-signal     (depends on core)
    ^
    |
hyperfun-executor   (depends on core)
    ^
    |
main.rs             (depends on all, wires via channels)
```

### Key Rust Dependencies

| Purpose | Crate |
|---------|-------|
| Async runtime | `tokio` |
| WebSocket | `tokio-tungstenite` |
| HTTP client | `reqwest` |
| Serialization | `serde` + `serde_json` |
| Configuration | `config` |
| Logging | `tracing` + `tracing-subscriber` |

No `axum` (no HTTP server), no `sqlx` (no DB), no `teloxide` (no Telegram), no `notify` (no hot reload).

## V1 Scope (Lean MVP)

**In scope**:
- MarketDataEngine: HL WebSocket candles + REST backfill + HL-native data
- SignalEngine: 6 factor groups (TA + HL-native), multi-timeframe, bar-close signals
- PaperExecutor: realistic fills, position tracking, PnL, console output
- Configuration: TOML config, no hot reload
- Tests: unit + integration + property tests (see test plan)

**NOT in scope (V2+)**:
- Live trading executor (requires private key, risk management)
- Full risk management suite (max positions, drawdown, circuit breaker)
- Backtesting engine (next priority after V1 validates signals)
- Dashboard (TypeScript frontend)
- Telegram notifications
- PostgreSQL persistence
- Docker deployment
- REST API
- Hot-reload configuration
- Custom candle aggregation from trades
- Trailing stop / partial take profit
- Multi-account support

## Warmup Behavior

On startup:
1. Connect to HL WebSocket, subscribe to candle streams for all symbols in watchlist
2. REST backfill: fetch last 500 candles per symbol per timeframe
3. Feed historical candles through all indicators to warm them up
4. Begin live signal generation only after ALL indicator groups report `ready()`
5. Log warmup duration and first signal timestamp

Expected warmup for default config (EMA50 on 15m): ~500 * 15min = ~5 days of history, backfilled in seconds via REST.

<!-- AUTONOMOUS DECISION LOG -->
## Decision Audit Trail

| # | Phase | Decision | Classification | Principle | Rationale | Rejected |
|---|-------|----------|---------------|-----------|-----------|----------|
| 1 | CEO | Mode: SELECTIVE EXPANSION | Mechanical | P1 | Standard mode | — |
| 2 | CEO | Enable cross-project learnings | Mechanical | P1 | Completeness | — |
| 3 | CEO | V1 scope: Lean MVP + HL signals | Premise (user) | — | User chose after dual-voice challenge | Full spec, backtesting V1 |
| 4 | Eng | Use HL candles not custom aggregation | Mechanical | P5 | Hidden complexity | Custom aggregation |
| 5 | Eng | All indicators in group must be ready | Mechanical | P5 | Partial readiness = wrong weights | Renormalize |
| 6 | Eng | Two traits: CandleIndicator + HlSignalProvider | Mechanical | P5 | Candle-only trait doesn't fit HL data | Single trait |
| 7 | Eng | Remove hot reload from MVP | Mechanical | P3 | Complexity trap | Keep hot reload |
| 8 | Eng | Signals only on bar close | Mechanical | P5 | Prevents churn | Signal on tick |
| 9 | Eng | Realistic paper fill model | Mechanical | P1 | Sole validation tool | Naive fills |
| 10 | Eng | No private key in paper mode | Mechanical | P3 | Security | Always load key |

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/gstack-plan-ceo-review` | Scope & strategy | 1 | clean | 0/6 premises confirmed; scope reduced to lean MVP |
| CEO Voices | `/gstack-autoplan` | Codex+subagent | 1 | clean | 0/6 consensus; both flagged premature infrastructure |
| Eng Review | `/gstack-plan-eng-review` | Architecture & tests | 1 | clean | 11 issues found, all resolved |
| Eng Voices | `/gstack-autoplan` | Codex+subagent | 1 | clean | 2/6 confirmed; 4 needed work |
| Design Review | `/gstack-plan-design-review` | UI/UX gaps | 0 | skipped | No UI in lean MVP |
| DX Review | `/gstack-plan-devex-review` | Developer experience | 0 | skipped | No dev-facing API in lean MVP |

**VERDICT:** APPROVED. Lean MVP scope validated by CEO + Eng dual voices. All 11 engineering findings resolved in revised spec.
