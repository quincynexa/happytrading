# Postgres Storage Design

**Date:** 2026-04-14
**Scope:** Sub-project #1 of the deployment arc (DB → Docker → Cloud).
**Status:** Design approved, pending implementation plan.

## Motivation

`hyperfun` currently writes `scores` and `trades` to JSONL files. This was
enough for post-hoc analysis, but is insufficient for the next phase:

1. **Durability / integrity** — JSONL is append-only and vulnerable to
   partial writes on crash. Atomic transactional writes are required.
2. **SQL analytics** — strategy optimization needs ad-hoc queries
   (factor / PnL correlation, win rate by symbol, etc.).
3. **State recovery on restart** — open positions, cooldown state, and
   warmed-up indicator state currently live only in memory. A crash or
   redeploy loses everything.

This spec replaces the JSONL writer with a Postgres-backed store while
keeping the JSONL as a fallback path when the database is unreachable.

## Non-Goals

- Live (real-money) trading. Paper mode only.
- Multi-instance / distributed deployment.
- Real-time dashboards. Analytics is batch / ad-hoc via SQL.

## Tech Stack

| Layer | Choice | Reason |
|---|---|---|
| Driver | `sqlx` 0.8 with Postgres feature | Native async, compile-time SQL checking, built-in migrations, integrates cleanly with tokio |
| Pool | `sqlx::PgPool`, size 5 | Single-process bot; 5 connections is generous headroom |
| Migrations | `sqlx migrate` with `migrations/*.sql` | Auto-run at startup; forward-only |
| Config | env var `DATABASE_URL` + `[storage]` block in `config/default.toml` | Secrets stay out of code; tunables stay in config |

A new workspace crate `crates/hyperfun-storage` encapsulates all DB access
behind a `Storage` struct. Writing a trait abstraction is deferred — the
concrete struct is sufficient until a second backend is actually needed.

## Schema

Seven tables.

### `bar_scores`
One row per bar-close evaluation per symbol.
```
id            BIGSERIAL PRIMARY KEY
ts            BIGINT NOT NULL            -- close_time, ms since epoch
symbol        TEXT NOT NULL
open_time     BIGINT NOT NULL
close_price   DOUBLE PRECISION NOT NULL
composite     DOUBLE PRECISION NOT NULL
trend         DOUBLE PRECISION            -- nullable (group not ready)
momentum      DOUBLE PRECISION
volatility    DOUBLE PRECISION
hl_native     DOUBLE PRECISION
funding       DOUBLE PRECISION
action        TEXT NOT NULL               -- 'open_long' | 'open_short' | 'close' | 'hold'
```
Index: `(symbol, open_time)` for time-series scans.

### `trades`
One row per open / close / stop-loss event.
```
id            BIGSERIAL PRIMARY KEY
ts            BIGINT NOT NULL
symbol        TEXT NOT NULL
event         TEXT NOT NULL               -- 'open' | 'close'
direction     TEXT NOT NULL               -- 'Long' | 'Short' | 'Unknown'
price         DOUBLE PRECISION NOT NULL
fill_price    DOUBLE PRECISION NOT NULL
composite     DOUBLE PRECISION NOT NULL
atr           DOUBLE PRECISION NOT NULL
stop_loss     DOUBLE PRECISION
pnl           DOUBLE PRECISION
reason        TEXT                        -- 'signal' | 'stop_loss' | 'trailing_stop' | 'direction_flip'
```
Index: `(symbol, ts)`.

### `positions`
Current open positions. Restored into `PaperExecutor` on startup.
```
symbol         TEXT PRIMARY KEY
direction      TEXT NOT NULL
size_usd       DOUBLE PRECISION NOT NULL
entry_price    DOUBLE PRECISION NOT NULL
entry_time     BIGINT NOT NULL
stop_loss      DOUBLE PRECISION NOT NULL
extreme_price  DOUBLE PRECISION NOT NULL
fees_paid      DOUBLE PRECISION NOT NULL
funding_paid   DOUBLE PRECISION NOT NULL
updated_at     BIGINT NOT NULL
```
Small table, typically <10 rows.

### `cooldowns`
Per-symbol cooldown expiry. Absolute timestamps, not bar counts.
```
symbol              TEXT PRIMARY KEY
cooldown_until_ts   BIGINT NOT NULL
```

### `market_data_snapshots`
Raw REST-polled data for replay / verification.
```
id          BIGSERIAL PRIMARY KEY
ts          BIGINT NOT NULL
symbol      TEXT
data_type   TEXT NOT NULL                 -- 'funding' | 'hlp' | 'whale' | 'oi'
payload     JSONB NOT NULL
```
Index: `(data_type, ts)`.

### `daily_stats`
Materialized daily aggregates per symbol.
```
date             DATE NOT NULL
symbol           TEXT NOT NULL
total_trades     INT NOT NULL
winning_trades   INT NOT NULL
losing_trades    INT NOT NULL
total_pnl        DOUBLE PRECISION NOT NULL
gross_profit     DOUBLE PRECISION NOT NULL
gross_loss       DOUBLE PRECISION NOT NULL
max_drawdown     DOUBLE PRECISION NOT NULL
PRIMARY KEY (date, symbol)
```

### `candle_cache`
Cached historical candles, replaces backfill fetches on restart.
```
symbol       TEXT NOT NULL
interval     TEXT NOT NULL
open_time    BIGINT NOT NULL
close_time   BIGINT NOT NULL
open         DOUBLE PRECISION NOT NULL
high         DOUBLE PRECISION NOT NULL
low          DOUBLE PRECISION NOT NULL
close        DOUBLE PRECISION NOT NULL
volume       DOUBLE PRECISION NOT NULL
num_trades   BIGINT NOT NULL
PRIMARY KEY (symbol, interval, open_time)
```

## Write Path

### Storage API

```rust
pub struct Storage {
    pool: Option<PgPool>,        // None after the first DB failure
    journal: JournalWriter,      // always-available fallback
}

impl Storage {
    pub async fn new(config: &AppConfig) -> Result<Self>;
    pub async fn write_score(&mut self, record: BarScoreRecord);
    pub async fn write_trade(&mut self, record: TradeRecord);
    pub async fn upsert_position(&mut self, pos: &Position);
    pub async fn delete_position(&mut self, symbol: &str);
    pub async fn upsert_cooldown(&mut self, symbol: &str, until_ts: i64);
    pub async fn write_market_snapshot(&mut self, data_type: &str, symbol: Option<&str>, ts: i64, payload: serde_json::Value);
    pub async fn upsert_candle(&mut self, c: &Candle);
    // Read-side API is in the "Startup Path" section.
}
```

### Fallback logic

On every write attempt:
1. If `pool` is `Some`, try the DB write.
2. On success, return.
3. On failure, `warn!` with error + record kind, set `pool = None`, then
   call the corresponding `JournalWriter` method.

Rationale: once the DB fails, subsequent writes go straight to JSONL
without repeatedly paying the DB timeout. Recovery is manual (process
restart re-establishes the pool). Automatic reconnection is explicitly
out of scope for V1.

### Write points in `main.rs`

Replace current `journal.write_*` calls with `storage.write_*`:
- `bar_scores` on every bar-close evaluation
- `trades` on every open / close / stop-loss event
- `positions`: upsert on open, delete on close
- `cooldowns`: upsert on `clear_position`
- `market_data_snapshots`: in each REST poller task
- `candle_cache`: on every bar-close (upsert the closed bar)

All writes are `.await`-ed synchronously in the existing `tokio::select!`
loop. At our volume (peak ~10 writes/sec during REST polling), Postgres
on localhost responds in single-digit ms — not a concern.

### Cooldown mechanics change

Current implementation in `SignalAggregator` counts down `remaining_bars`
on each `decide()` call. This is lost on restart. Switching to an
absolute-timestamp scheme:

- `cooldown_bars` in config stays in "bars" units (e.g. 3).
- `SignalAggregator::new` takes an additional `entry_interval_ms`
  parameter (e.g. 900_000 for 15m).
- On `clear_position`, compute `cooldown_until_ts = current_ts + cooldown_bars * entry_interval_ms`
  and persist via `storage.upsert_cooldown`.
- On `decide`, compare `current_ts < cooldown_until_ts` instead of
  decrementing a counter.
- `current_ts` is passed into `decide()` (new parameter); callers already
  have `closed_ts` available.

## Startup Path

Startup order in `main.rs`:

1. Load config.
2. Init `Storage`: connect pool, run migrations.
3. `storage.load_candles()` → populate `CandleStore`.
4. **Incremental backfill**: for each `(symbol, interval)`, query HL REST
   for the range `[max_cached_close_time + 1, now]`. If the cache was
   empty, this falls through to the current "500-candle fetch" behavior.
5. Warm up indicators from `CandleStore` (unchanged logic).
6. `storage.load_positions()` → for each row, restore into
   `PaperExecutor` (new public method `restore_position`) and mirror into
   `SignalAggregator` via `set_position`.
7. `storage.load_cooldowns()` → populate `SignalAggregator`'s internal
   cooldown map.
8. Spawn REST pollers and WS client.
9. Enter main loop.

### `PaperExecutor::restore_position`

New public method that inserts a pre-built `Position` into the internal
`HashMap`. Does not touch `RunningStats`. Used only by the startup
recovery path.

### Incremental backfill

Currently `MarketDataEngine::backfill` fetches 500 candles unconditionally.
It will accept an optional `start_ms_override: Option<i64>` derived from
the cache's latest close_time per (symbol, interval).

Three cases:
- **Cache empty or <500 bars**: ignore the override, do the current
  full 500-bar fetch. Indicators need adequate history for warmup.
- **Cache ≥500 bars**: fetch only `[max_cached_close_time + 1, now]`.
- **Cache has gap** (e.g. process was down for hours): incremental
  fetch covers the gap automatically.

The threshold of "500 bars" matches the `CandleStore` capacity and
the warmup length used by indicators with the longest period
(MACD needs slow+signal ≈ 35, EMA long period = 50; 500 is generous).

## Background Tasks

One tokio task spawned in `main.rs` with `tokio::time::interval(24h)`.
Aligned to UTC 00:00 at startup.

At each tick:
1. **00:05 — daily_stats rollup**
   Aggregate `trades` for the previous UTC day, upsert into `daily_stats`.
   Query: `SELECT date, symbol, COUNT(*), ..., SUM(pnl) FROM trades WHERE ts BETWEEN ... GROUP BY date, symbol`.
   Only rolls up closed trades (`event = 'close'` with non-null `pnl`).

2. **00:10 — retention pruning**
   Run the DELETE queries below.

### Retention policy

| Table | Retention | Rationale |
|---|---|---|
| `bar_scores` | 90 days | Used for scoring analysis; old data loses relevance as strategy evolves |
| `trades` | forever | Low volume, core historical record |
| `positions` | permanent (current state) | Self-trimming |
| `cooldowns` | permanent | Self-trimming, <10 rows |
| `market_data_snapshots` | 30 days | Highest-volume table; long-term replay not needed |
| `daily_stats` | forever | Small, high-value |
| `candle_cache` | 1 year | Plenty for a 500-bar warmup on any supported interval |

### On-demand fallback for `daily_stats`

`daily_stats` for *today* won't exist until the next 00:05 rollup. Any
query for the current day should compute from `trades` directly.
(This is a consumer-side concern — no code change in the bot itself.)

## Config Changes

### `config/default.toml`

```toml
[storage]
pool_size = 5
# DATABASE_URL comes from env

[signal]
# ... existing fields ...
# cooldown_bars semantics unchanged from user's perspective
```

### `crates/hyperfun-core/src/config.rs`

Add `StorageConfig { pool_size: u32 }`.

## Error Handling

- **Migration failure at startup**: fatal, process exits with error. Can't
  run a misaligned schema.
- **Pool connection failure at startup**: fatal in V1. Fallback-to-JSONL
  only kicks in for write failures during the main loop, not for
  initialization.
- **Write failure during main loop**: warn log + set `pool = None` + fall
  through to JSONL. No retry.
- **Read failure during startup (load_candles / load_positions / load_cooldowns)**:
  warn log + proceed with empty state (same behavior as when cache is cold).

## Testing

1. **Unit tests in `hyperfun-storage`** — schema correctness:
   for each table, insert a row and read it back. Requires a test DB.
   Use `sqlx::test` macro with `DATABASE_URL_TEST` env var.

2. **Integration test**: cold-start → write 5 scores / 2 trades / 1 position /
   1 cooldown → restart from same DB → assert state matches.

3. **Fallback test**: initialize `Storage` with `pool = None` → call
   every write method → assert JSONL files receive the records.

4. **Migration test**: run migrations forward on an empty DB, assert all
   7 tables exist with expected columns.

## Implementation Order

When the implementation plan is written, rough ordering:

1. New `hyperfun-storage` crate + migrations + `PgPool` wiring + config.
2. Replace `JournalWriter` calls in `main.rs` with `Storage` (keep
   fallback path).
3. Cooldown refactor (bar count → absolute timestamp).
4. `positions` / `cooldowns` persistence + startup recovery.
5. `candle_cache` + incremental backfill.
6. `market_data_snapshots` writes in REST pollers.
7. Background task: daily_stats rollup + retention pruning.
8. Tests.

## Out of Scope (for later sub-projects)

- Docker containerization — sub-project #2.
- Cloud deployment + secrets management — sub-project #3.
- Migration to real-money trading — separate design.
- Automated DB reconnection after failure.
- Metrics / dashboards (Prometheus, Grafana).
