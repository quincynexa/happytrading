<!-- /autoplan restore point: ~/.gstack/projects/quincynexa-happytrading/feat-lean-mvp-autoplan-restore-20260414-225816.md -->
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

Seven tables. All enum-valued TEXT columns get CHECK constraints. All
retention-pruned tables get a dedicated `ts`-only index. All tables that
can be re-inserted on restart get a UNIQUE constraint to enable
idempotent upserts.

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
action        TEXT NOT NULL CHECK (action IN ('open_long','open_short','close','hold'))
UNIQUE (symbol, open_time)
```
Indexes: `(symbol, open_time)` and `(ts)` for retention.
Writes use `INSERT ... ON CONFLICT (symbol, open_time) DO NOTHING`.

### `trades`
One row per open / close / stop-loss event.
```
id            BIGSERIAL PRIMARY KEY
ts            BIGINT NOT NULL
symbol        TEXT NOT NULL
event         TEXT NOT NULL CHECK (event IN ('open','close'))
direction     TEXT NOT NULL CHECK (direction IN ('Long','Short','Unknown'))
price         DOUBLE PRECISION NOT NULL
fill_price    DOUBLE PRECISION NOT NULL
composite     DOUBLE PRECISION NOT NULL
atr           DOUBLE PRECISION NOT NULL
stop_loss     DOUBLE PRECISION
pnl           DOUBLE PRECISION
reason        TEXT CHECK (reason IN ('signal','stop_loss','trailing_stop','direction_flip'))
UNIQUE (symbol, ts, event)
```
Indexes: `(symbol, ts)` and `(ts)` for retention rollup / scans.
Writes use `INSERT ... ON CONFLICT (symbol, ts, event) DO NOTHING`.

### `positions`
Current open positions. Restored into `PaperExecutor` on startup.
```
symbol         TEXT PRIMARY KEY
direction      TEXT NOT NULL CHECK (direction IN ('Long','Short'))
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
Per-symbol cooldown expiry. Driven by **bar `close_time`** (market time),
not wall clock.
```
symbol              TEXT PRIMARY KEY
cooldown_until_ts   BIGINT NOT NULL   -- in the same timebase as candle close_time
cooldown_bars       INT NOT NULL       -- config value at write time (for config-change detection)
updated_at          BIGINT NOT NULL    -- ms wall clock
```
The `cooldown_bars` column lets startup detect a config change: if the
loaded value differs from the current `config.signal.cooldown_bars`, we
drop that cooldown row (documented rule: cooldown-bar changes take
effect on the next close, not retroactively).

### `market_data_snapshots`
Raw REST-polled data for replay / verification.
```
id          BIGSERIAL PRIMARY KEY
ts          BIGINT NOT NULL
symbol      TEXT
data_type   TEXT NOT NULL CHECK (data_type IN ('funding','hlp','whale','oi'))
payload     JSONB NOT NULL
CHECK (data_type = 'oi' OR symbol IS NOT NULL)
```
Indexes: `(data_type, ts)` for query, `(ts)` alone for retention DELETE.

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
Additional index: `(symbol, interval, close_time DESC)` for fast
latest-close lookup during incremental backfill.

## Write Path

### Architecture: Writer Task + Typed Ops

The main `tokio::select!` loop MUST NOT block on DB writes. Cloud
Postgres RTT (20-50ms) multiplied across multiple writes per bar would
backpressure the bounded `candle_rx` channel and delay stop-loss
handling. Solution: a dedicated writer task behind an mpsc channel.

```rust
pub enum WriteOp {
    BarScore(BarScoreRecord),
    // Stateful transitions — each variant is one SQL transaction
    OpenTrade { trade: TradeRecord, position: Position },
    CloseTrade { trade: TradeRecord, symbol: String, cooldown_until: i64, cooldown_bars: u32 },
    FlipTrade { close_trade: TradeRecord, open_trade: TradeRecord, new_position: Position, cooldown_until: i64, cooldown_bars: u32 },
    // Non-stateful
    MarketSnapshot { data_type: String, symbol: Option<String>, ts: i64, payload: serde_json::Value },
    CandleClose(Candle),
}

pub struct StorageHandle {
    tx: mpsc::Sender<WriteOp>,
}

impl StorageHandle {
    pub fn try_send(&self, op: WriteOp) -> Result<(), TrySendError<WriteOp>>;
    // Main loop uses try_send; if channel is full, log warn and drop
    // (bar_scores / market_snapshots are non-critical). Stateful ops
    // (OpenTrade/CloseTrade/FlipTrade) use blocking send with a timeout.
}

pub struct StorageWriter {
    rx: mpsc::Receiver<WriteOp>,
    pool: Arc<RwLock<Option<PgPool>>>,
    journal: JournalWriter,
}
```

The main loop owns `StorageHandle` (cheap clone). The `StorageWriter`
task is spawned once at startup, consumes `WriteOp`s, and executes each
as a single transaction.

Channel capacity: 1024 (generous headroom; at peak 20 writes/sec, this
is >50 seconds of buffer for a stalled DB).

### Transactional Domain Methods

Each stateful `WriteOp` variant maps to exactly one SQL transaction:

- **`OpenTrade`** — `BEGIN; INSERT trades; INSERT positions; COMMIT;`
- **`CloseTrade`** — `BEGIN; INSERT trades; DELETE positions WHERE symbol=?; UPSERT cooldowns; COMMIT;`
- **`FlipTrade`** — `BEGIN; INSERT trades (close); DELETE positions; INSERT trades (open); INSERT positions; UPSERT cooldowns; COMMIT;`

This eliminates the impossible-state class of bugs where a crash
between statements leaves `trades` showing a close but `positions` still
has the row.

Non-stateful ops (`BarScore`, `MarketSnapshot`, `CandleClose`) are single
`INSERT ... ON CONFLICT DO NOTHING` statements, no transaction needed.

### Fallback Logic

The writer task holds `pool` inside `Arc<RwLock<Option<PgPool>>>`. On any
DB error:
1. `warn!` with error + op kind
2. Set `*pool.write() = None`
3. Write the op via `JournalWriter` fallback (see journal extension below)

A 60-second health-check task (spawned alongside the writer) periodically
attempts `SELECT 1`; on success, reinstate the pool. This replaces the
"manual restart only" recovery in the original spec.

### Journal Fallback — Expanded Coverage

The existing `JournalWriter` covers `BarScore` and `TradeRecord`. Extend
to cover stateful transitions:

- `data/positions.jsonl` — append on every `OpenTrade` / `CloseTrade` /
  `FlipTrade` (one line per position mutation)
- `data/cooldowns.jsonl` — append on cooldown updates
- `data/market_snapshots.jsonl` — append on market snapshots
- `data/candles.jsonl` — append on candle closes

On DB recovery, these JSONL files can be replayed (out of scope for V1 —
documented as operational runbook). For V1, the guarantee is: **no data
loss**, even if recovery is manual.

### Write Points in main.rs

- Replace `journal.write_score(...)` with `storage_handle.try_send(WriteOp::BarScore(...))`
- On real open event: `storage_handle.send(WriteOp::OpenTrade { ... }).await`
- On real close event: `storage_handle.send(WriteOp::CloseTrade { ... }).await`
- On flip event: single `WriteOp::FlipTrade` (NOT two separate ops)

Critically: `aggregator.clear_position(symbol)` is currently called every
bar-close when `!executor.has_position(symbol)` — even when no real close
just happened. **This must be changed** so cooldown is only persisted on
an actual `had_position → !has_position` state transition. The executor
will return a new `TradeEvent` enum explicitly indicating what happened:

```rust
pub enum TradeEvent {
    None,
    Opened { position: Position, trade: TradeRecord },
    Closed { trade: TradeRecord, pnl: f64 },
    Flipped { close_trade: TradeRecord, open_trade: TradeRecord, new_position: Position, pnl: f64 },
    StopLossTriggered { trade: TradeRecord, pnl: f64 },
}
```

Main loop matches on this enum to decide which `WriteOp` (if any) to
send. This fixes the cooldown write-amplification bug identified in
Phase 3 review.

### Cooldown Mechanics

Cooldown is driven by **bar close_time**, not wall clock:

- `cooldown_bars` in config stays in "bars" units (e.g. 3).
- `SignalAggregator::new` takes `entry_interval_ms` (e.g. 900_000 for 15m).
- On **real close event only** (not every flat bar): compute
  `cooldown_until_ts = closed_ts + cooldown_bars * entry_interval_ms`.
  Persisted along with `cooldown_bars` value and `updated_at` wall-clock.
- On `decide(symbol, composite_score, current_bar_close_ts)`: compare
  `current_bar_close_ts < cooldown_until_ts`.

This eliminates clock-drift issues and keeps cooldown semantics
consistent across restarts.

## Startup Path

Startup order in `main.rs`:

1. Load config.
2. Init `Storage`: connect pool, run migrations.
3. `storage.load_candles_bulk()` — **single query** (see N+1 note below).
4. **Incremental backfill with gap detection** (see below).
5. Warm up indicators from `CandleStore`.
6. `storage.load_positions()` — **bulk query, load-failure is FATAL**
   (trading state, not cache).
7. `storage.load_cooldowns()` — **bulk query, load-failure is FATAL**.
   Drop rows whose `cooldown_bars != current config value` (config
   change detection; documented "takes effect on next close" rule).
8. Spawn `StorageWriter` task, REST pollers, WS client.
9. Enter main loop.

### Startup failure modes

| Stage | Failure mode | Behavior |
|---|---|---|
| 2. Pool connect | DB unreachable | **Fail-open**: start with `pool = None`, loud warn log, all writes go to JSONL fallback until background reconnect succeeds |
| 2. Migrations | Migration file errors | **Fatal**: can't run against partial schema |
| 3. load_candles | Query error | Warn + proceed with empty cache (falls through to full backfill) |
| 4. REST backfill | HL API down | Warn + proceed with whatever cached data exists |
| 6. load_positions | Query error OR row deserialize error | **Fatal**: trading state integrity required. Exit with clear error. |
| 7. load_cooldowns | Query error OR row deserialize error | **Fatal**: same rationale as positions. |

Row-level partial reload: if any row in `positions` or `cooldowns` fails
to deserialize (NaN, bad enum), treat as fatal. These tables are small
(<20 rows total); better to refuse to start than trade with partial state.

The pool-connect fail-open is the only "continue with degraded mode"
case. Matches the fallback philosophy: JSONL is always available, so DB
outage shouldn't stop trading (but restart recovery is now dependent on
the JSONL replay runbook).

### N+1 Query Elimination

- `load_candles_bulk()`: single `SELECT * FROM candle_cache ORDER BY symbol, interval, open_time` (streaming cursor via `sqlx::query_as`), partitioned into `CandleStore` by (symbol, interval)
- `load_positions()`: single `SELECT * FROM positions`
- `load_cooldowns()`: single `SELECT * FROM cooldowns`
- Incremental backfill metadata: one query —
  `SELECT symbol, interval, MAX(close_time) AS max_ct, COUNT(*) AS cnt FROM candle_cache GROUP BY symbol, interval`

### `PaperExecutor::restore_position`

New public method that inserts a pre-built `Position` into the internal
`HashMap`. Does not touch `RunningStats`. Used only by the startup
recovery path.

### Incremental Backfill with Gap Detection

For each `(symbol, interval)`, after `load_candles_bulk`:

1. **Coverage check**: compute `expected_bar_count = (now - earliest_cached_open_time) / interval_ms`. If `cached_count < 0.95 * expected_bar_count`, there are gaps in the middle — refetch the full 500 from REST (gap-detection fallback).
2. **Warmup adequacy**: if `cached_count < 100` (or less than longest indicator period × 2), ignore cache entirely, do full 500-bar fetch.
3. **Incremental top-up**: if cache is contiguous and adequate, fetch only `[max_cached_close_time + 1, now]`.

Case summary:
- **Cache empty**: full 500-bar fetch (identity with current behavior)
- **Cache partial/sparse**: full 500-bar fetch (reset)
- **Cache contiguous + adequate**: incremental top-up only
- **Cache contiguous + adequate + stale (hours down)**: incremental top-up covers the gap

The gap threshold (`< 95%` of expected bars) is intentionally
conservative — we'd rather refetch than warm indicators with wrong data.

### Config Change: cooldown_bars

If the persisted `cooldowns.cooldown_bars` column differs from the
current `config.signal.cooldown_bars`, drop that row on load. Documented
rule: **changes to `cooldown_bars` take effect on the next close, not
retroactively.** This keeps the state model simple and predictable.

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

### Retention DELETE Batching

Large DELETEs lock the table and bloat WAL. Use batched deletion:

```sql
DELETE FROM market_data_snapshots
WHERE ctid IN (
    SELECT ctid FROM market_data_snapshots
    WHERE ts < $1
    LIMIT 10000
);
```

Loop until `rows_affected = 0`, with a brief sleep between batches. Each
retention-pruned table has a dedicated `ts`-only index to make this
non-scanning: `idx_bar_scores_ts`, `idx_market_data_snapshots_ts`,
`idx_candle_cache_close_time` (already part of the compound index).

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

See the Startup failure modes table above for a complete matrix. Summary:

- **Migration failure at startup**: fatal.
- **Pool connection failure at startup**: fail-open (pool = None, JSONL
  mode, background reconnect task attempts recovery every 60s).
- **Write failure during main loop**: warn + flip pool to None + JSONL
  fallback + background reconnect task tries to restore.
- **Read failure for candle_cache**: warn + empty state → triggers full
  backfill.
- **Read failure for positions or cooldowns**: FATAL. Exit with clear
  error. These are trading state, not cache.

### DATABASE_URL Handling

Parse `DATABASE_URL` into `PgConnectOptions` via
`PgConnectOptions::from_str`, then connect via `PgPoolOptions`. This
avoids sqlx's default behavior of including the full URL (with password)
in connection error messages. A redaction test asserts that no log line
contains the URL's password component.

## Testing

### Infrastructure

- `docker-compose.test.yml` with Postgres 15
- `DATABASE_URL_TEST` env var (separate DB from dev)
- GitHub Actions: Postgres service container in CI
- `.sqlx/` offline query cache checked into git (for CI without live DB at compile time)
- Each `sqlx::test`-marked function gets a clean database

### Required Tests (priority order)

1. **Cooldown write-amplification regression** — Fire 100 bar-close events
   where symbol has no position. Assert `cooldowns` row count stays at 0
   (or unchanged if previously set). This test will fail on the original
   spec design. Proves the state-transition fix.

2. **Transaction atomicity** — Start an `OpenTrade` transaction; inject
   DB failure between `INSERT trades` and `INSERT positions`. Restart,
   assert no row exists in either table (rolled back), not just one.

3. **Cross-process recovery** — Test harness with two tokio tasks
   sharing a DB: task 1 opens a position + closes it, task 2 restarts
   with same DB, asserts `executor.get_position` and `aggregator.has_position`
   are consistent with task 1's final state, including cooldown.

4. **Fail-open startup** — Point `DATABASE_URL` at an unreachable host.
   Assert the process starts, logs a loud warning, and writes go to
   JSONL fallback.

5. **Fallback transition mid-write** — Pool = Some, inject DB failure
   on a specific write. Assert: exactly one JSONL line written, pool
   flipped to None, subsequent writes also in JSONL.

6. **Background reconnect** — Start fallback mode, bring DB back up,
   assert within 60s the pool reconnects and new writes go to DB.

7. **Cooldown boundary timing** — Close position at bar `t=100`, cooldown
   until `t=145` (3×15min). Feed bar at `t=144` → blocked. Bar at `t=145`
   → released. Deterministic.

8. **Cooldown config change on restart** — Persist `cooldowns.cooldown_bars=3`,
   restart with config `cooldown_bars=5`, assert the persisted row is
   dropped on load (new cooldowns use new value).

9. **Migration idempotency** — Run migrations, then run them again,
   assert second run is a no-op (no errors, no schema changes).

10. **Candle gap detection** — Insert `candle_cache` with a 2-hour hole
    in the middle, start bot, assert incremental backfill detects gap
    via the 95% coverage check and refetches the full 500-bar range.

11. **Row-level corruption = fatal for state tables** — Insert a row in
    `positions` with NaN `entry_price`, start bot, assert process exits
    with clear error (NOT silently skips the row).

12. **`DATABASE_URL` redaction** — Cause a connect failure with a URL
    containing a password. Grep all log output, assert password string
    does not appear.

13. **UNIQUE constraint** — Insert duplicate `(symbol, open_time)` into
    `bar_scores` via two concurrent tokio tasks. Assert both succeed
    (ON CONFLICT DO NOTHING) and only one row results.

14. **CHECK constraint** — Attempt to insert `trades.direction = 'Both'`.
    Assert DB returns a constraint violation error.

15. **Retention batching** — Populate 100k rows in `market_data_snapshots`
    with old `ts`, run retention, assert deletion completes without
    locking other writers and in batches.

### Test Module Layout

- `crates/hyperfun-storage/tests/migrations.rs` — migration tests
- `crates/hyperfun-storage/tests/schema.rs` — CHECK / UNIQUE tests
- `crates/hyperfun-storage/tests/recovery.rs` — cross-process recovery
- `crates/hyperfun-storage/tests/fallback.rs` — JSONL fallback tests
- `crates/hyperfun-storage/tests/cooldown.rs` — cooldown regression + boundary tests
- `tests/integration_storage.rs` (workspace-level) — wire up with main.rs flow

## Implementation Order

1. **Infrastructure**: `hyperfun-storage` crate, migrations directory,
   `PgPool` wiring, config changes, `docker-compose.test.yml`, CI
   Postgres service, `.sqlx/` offline cache.
2. **Schema migrations**: all 7 tables with CHECK / UNIQUE / indexes.
3. **Writer task**: `StorageWriter` with `WriteOp` enum + mpsc channel.
   All non-stateful ops (BarScore, MarketSnapshot, CandleClose) writing
   to DB with fallback to JSONL.
4. **`TradeEvent` enum in executor + aggregator rewiring**:
   - `PaperExecutor::execute_signal` returns `TradeEvent`
   - `check_stop_losses` returns `TradeEvent::StopLossTriggered | None`
   - Main loop matches on `TradeEvent`, dispatches correct `WriteOp`
   - **Fixes cooldown write-amplification bug.**
5. **Cooldown refactor**: bar count → absolute `close_time`-based
   timestamp, with config-change detection on load.
6. **Transactional domain ops** (`OpenTrade`, `CloseTrade`, `FlipTrade`):
   all wrapped in `BEGIN;...COMMIT;`.
7. **Startup recovery**: bulk load candle_cache (with gap detection),
   positions (fatal on failure), cooldowns (fatal on failure).
8. **Incremental backfill with gap detection** (95% coverage threshold).
9. **Background reconnect** task (60s SELECT 1).
10. **Daily stats rollup + retention pruning** (batched DELETEs).
11. **Tests** — the 15-scenario suite.

## Out of Scope (for later sub-projects)

- Docker containerization — sub-project #2.
- Cloud deployment + secrets management — sub-project #3.
- Migration to real-money trading — separate design.
- Automated DB reconnection after failure.
- Metrics / dashboards (Prometheus, Grafana).

---

# /autoplan Phase 1 — CEO Review

## Dual Voices

### CLAUDE SUBAGENT (CEO — strategic independence)

1. **Wrong problem, wrong time (CRITICAL)** — 24 bar evaluations, 0 trades. Persistence is hypothetical until strategy actually fires. Gate this work on ≥50 real paper trades.
2. **Postgres vs SQLite — wrong tool (HIGH)** — Single-process, single-writer, solo-operator. SQLite is objectively better: zero ops, atomic, same SQL surface, embeddable in Docker, no fallback needed. Only Postgres advantage ("multi-reader") is explicitly ruled out in Non-Goals.
3. **Unexamined premises (HIGH)** — "State recovery matters" (losing state costs ~2h in dev), "SQL analytics unblocks optimization" (DuckDB over JSONL does this today), "Localhost-ms benchmark" (cloud Postgres won't match).
4. **6-month regret (HIGH)** — 7 tables + rollup + retention + dual-write for a strategy that never traded. `market_data_snapshots` as JSONB at 30d retention balloons to tens of GB.
5. **Opportunity cost (CRITICAL)** — 2-3 weeks solo on storage. Same time on replay harness or "why 0 trades" instrumentation directly moves toward the goal.

### CODEX SAYS (CEO — strategy challenge)

- **Direct conflict with MVP thesis**: `docs/superpowers/specs/2026-04-12-hyperfun-trading-bot-design.md:6` explicitly says "No PostgreSQL. No Docker. Prove positive expectancy before infrastructure."
- Core premises asserted, not demonstrated. No evidence persistence is the bottleneck.
- SQLite in WAL mode gives atomic commits, SQL queries, restartable state. Postgres adds service management, connection pooling, backup, Docker complexity — for no present benefit.
- 7 tables = warehouse thinking before strategy is validated. `daily_stats` materializing metrics for a system that hasn't produced trades.
- Fallback structurally bad: after one DB failure, flips to JSONL permanently until restart → split-brain history, undermines the "integrity" argument.
- Likely regret: production-shaped bot around a strategy that never cleared the trade threshold.

## CEO CONSENSUS TABLE

| Dimension | Claude | Codex | Consensus |
|---|---|---|---|
| 1. Premises valid? | NO | NO | **CONFIRMED: premises unexamined** |
| 2. Right problem to solve NOW? | NO | NO | **CONFIRMED: wrong priority** |
| 3. Scope calibration correct? | NO (too big) | NO (too big) | **CONFIRMED: scope bloated** |
| 4. Alternatives sufficiently explored? | NO (SQLite dismissed) | NO (SQLite is correct tool) | **CONFIRMED: tool mismatch** |
| 5. Market/competitive risks covered? | N/A | N/A | N/A |
| 6. 6-month trajectory sound? | NO | NO | **CONFIRMED: will regret building this now** |

**Overall: 5/5 confirmed disagreements with the spec as written.** Not a taste decision — both models independently recommend significant change.

## What Already Exists

- `src/journal.rs` — JSONL writer with flush-on-every-write (durable enough for current volume)
- `MarketDataEngine::backfill()` — REST-based historical candle loading
- `PaperExecutor` state in memory (positions, stats)
- `SignalAggregator` cooldown in memory (absolute-timestamp-ready if needed)
- Original MVP spec explicitly says "No PostgreSQL. No Docker." until expectancy is proven

## Alternatives Not Explored in Spec

| Approach | Effort (CC) | Coverage | Note |
|---|---|---|---|
| **SQLite + 3 tables** (trades, positions, cooldowns) | ~2 days | State recovery + durable trades | Proposed by both voices |
| **JSONL → DuckDB analytics** | 0 code changes | SQL analytics without DB changes | DuckDB can query JSONL directly |
| **Full Postgres as written** | ~2-3 weeks | All 3 goals (A+B+C) | The current spec |
| **Defer entirely** | 0 | 0 | Build backtest/replay harness first |

## 6-Month Dream State Delta

- **CURRENT**: Paper bot runs locally, 0 trades executed, JSONL logging
- **THIS SPEC**: + Postgres + 7 tables + state recovery + daily_stats + retention (2-3 weeks work)
- **12-MONTH IDEAL**: Live trading with validated edge, maybe multi-strategy, backtest loop proves new ideas in hours

Gap: this spec does NOT advance toward 12-month ideal. It builds infrastructure for a strategy that hasn't proven it works.

## Premise Gate — USER DECISION LOGGED

**User response:** Proceed with Postgres as written.
**Reasoning:** User accepts reviewers' concerns but maintains original direction (user sovereignty — models may be missing context on learning Postgres ops / cloud-consistency goals / future multi-bot plans).
**Consequence:** Phase 1 concerns are noted but do NOT block implementation. Phase 3 (Eng) will still surface architectural issues to address within the chosen direction.

---

# /autoplan Phase 3 — Eng Review

## Dual Voices

### CLAUDE SUBAGENT (Eng — independent architectural review)

Found 29 issues (F1-F29) across 7 dimensions. Highlights:

**HIGH — Architecture**:
- **F1**: `&mut self` on every write serializes the hot loop. Cloud Postgres RTT 20-50ms × multiple writes per bar will backpressure `candle_rx` (bounded 256). Use `Arc<Storage>` with `AtomicBool` / `RwLock<Option<PgPool>>`, or dedicated writer task via `mpsc<WriteOp>`.
- **F9**: DB unreachable at startup is "fatal in V1" but JSONL fallback exists for the exact same failure mode during runtime. Fail-open: start with `pool = None`, log loud warning, continue.

**HIGH — Edge cases**:
- **F4/F10**: Cooldown restart interaction. If `cooldown_bars` config changes between runs, persisted `until_ts` no longer reflects new rule. Compare against bar `close_time` not wall clock; on load, drop cooldown where `until_ts < now - 1h` as stale.
- **F5**: Partial position reload not addressed at row level. Spec says whole-query failure, but what about NaN price, bad direction string? Per-row: skip + warn.
- **F6**: `candle_cache` incremental backfill doesn't detect gaps. If process killed mid-WS-disconnect, the middle of the history has a hole but `max_close_time` is fresh. Indicators warm from gapped series → subtly wrong.

**HIGH — Testing**:
- **F12**: Testing section is a 4-line stub. Missing: fallback transition test (pool→None mid-write), cross-process recovery test, cooldown persistence test, migration idempotency test, candle gap detection test, `sqlx prepare` / offline mode handling.
- **F13**: `sqlx::test` needs `DATABASE_URL_TEST` + CI Postgres. Without it, storage layer ships untested.

**HIGH — Performance**:
- **F14**: Pool size 5 is useless when writes are serialized through `&mut self`.
- **F15**: Per-bar writes (bar_score + candle_cache + trade + position + cooldown) are not wrapped in a transaction. Mid-crash = impossible state. Atomicity rationale in §Motivation is false without this.

**MEDIUM — Schema**:
- **F22**: `bar_scores` missing `UNIQUE (symbol, open_time)` → duplicate rows after restart + re-evaluation. Use `INSERT ... ON CONFLICT DO NOTHING`.
- **F16**: `market_data_snapshots` retention DELETE can't use `(data_type, ts)` index for `WHERE ts < X`. Add `idx_mds_ts`.
- **F19/F27**: No CHECK constraints on enum-like TEXT (`data_type`, `event`, `direction`, `action`) — lose query enum safety.
- **F24**: `cooldowns` missing `updated_at` → can't detect stale rows after config change.

**MEDIUM — Security**:
- **F18**: Default sqlx errors can log full `DATABASE_URL` including password. Use `PgConnectOptions` from parts, redact on failure.
- **F21**: Retention DELETE unbounded → WAL bloat when pruning 900MB at once. Use `LIMIT 10000` in loop.

### CODEX SAYS (Eng — architecture challenge)

Found 8 issues, with two CRITICAL overlapping Claude's findings plus one Claude missed:

**CRITICAL #1 (matches Claude F15)** — Non-transactional multi-write paths. Trade + position + cooldown as separate SQL statements. Crash between them = impossible state (trade closed but position still open, or position deleted but no cooldown). Fix: transactional domain methods `record_open`, `record_close`, `record_flip`, `record_stop_close`.

**CRITICAL #2 (NEW — Claude missed this)** — **Write amplification bug from existing code interaction**. `src/main.rs:375-397` calls `aggregator.clear_position(&symbol)` on every bar-close when `!executor.has_position(&symbol)` — not only on actual close events. With the spec's persisted-cooldown-on-clear logic, this means: **every bar where symbol is flat rewrites cooldown with fresh `until_ts = now + N*interval`** → permanent cooldown extension → **bot will never open a position again after first close**. Fix: executor must return explicit state transition; only persist cooldown on real `had_position → !has_position`.

**HIGH #3 (matches Claude F9)** — "Warn and proceed with empty state" for `load_positions`/`load_cooldowns` is unsafe. These are trading state, not cache. Treat load failure as fatal OR start in read-only mode.

**HIGH #4 (matches Claude F1/F14)** — Synchronous await in tokio::select! loop. Dedicated writer task behind bounded channel; batch non-stateful writes; keep stateful transitions transactional.

**HIGH #5 (matches Claude F11)** — Cooldown refactor vulnerable to clock drift + config change boundaries. Persist against market-derived bar time, not wall clock. Compare using candle timestamps from same feed.

**MEDIUM #6 (matches Claude F22/F16)** — Missing indexes for stated queries. Add `trades(ts)`, `bar_scores(ts)`, `market_data_snapshots(ts)`, `candle_cache(symbol, interval, close_time DESC)`.

**MEDIUM #7 (matches Claude F12)** — Fallback only implemented for scores/trades. Positions, cooldowns, candle_cache, market_snapshots have no replay path. After DB failure, restart recovery is incomplete.

**MEDIUM #8 (matches Claude F8)** — Hidden N+1 risk in startup reload. Require streaming candle load + grouped metadata query; bulk positions/cooldowns in single queries.

## ENG CONSENSUS TABLE

| Dimension | Claude | Codex | Consensus |
|---|---|---|---|
| 1. Architecture sound? | NO (F1 hot loop) | NO (serial awaits) | **CONFIRMED: writer task needed** |
| 2. Test coverage sufficient? | NO (F12 stub) | NO (fallback weak) | **CONFIRMED: test plan incomplete** |
| 3. Performance risks addressed? | NO (F14 serialized) | NO (stalls loop) | **CONFIRMED: will cause backpressure** |
| 4. Security threats covered? | NO (F18 URL logging) | N/A | Claude-only; valid |
| 5. Error paths handled? | NO (F15 no tx) | NO (multi-write race) | **CONFIRMED CRITICAL: transactions required** |
| 6. Deployment risk manageable? | NO (F9 fatal startup) | NO (startup unsafe) | **CONFIRMED: fail-open + state mode** |
| BONUS from Codex | - | **Cooldown write amplification bug** | **CONFIRMED CRITICAL: would break bot** |

**Overall: 6/6 + 1 bonus CRITICAL. Both voices agreed the spec needs significant hardening before implementation.**

## Architecture — Proposed Dependency Graph

```
┌─────────────────────────────────────────────────────────┐
│                     main.rs (tokio::select!)            │
└─────────────────────────────────────────────────────────┘
      │                              │
      ▼                              ▼
┌──────────────┐         ┌─────────────────────┐
│  WS/REST     │         │  bar-close branch   │
│  pollers     │         │  - update indicators│
└──────────────┘         │  - decide() action  │
      │                  │  - executor action  │
      ▼                  └──────────┬──────────┘
      │                             │
      │  ┌──────────────────────────┼────────────────────┐
      └─▶│  mpsc<WriteOp> channel (bounded, e.g. 1024)   │
         └───────────────────────────┬───────────────────┘
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │   Storage Writer Task│
                          │  - consumes WriteOp  │
                          │  - batches ops       │
                          │  - transactional     │
                          │    boundaries per    │
                          │    trade event       │
                          │  - pool: Arc<PgPool> │
                          │  - fallback journal  │
                          └──────────┬───────────┘
                                     │
                     ┌───────────────┼───────────────┐
                     ▼                               ▼
              ┌─────────────┐              ┌─────────────────┐
              │  Postgres   │              │  JSONL fallback │
              └─────────────┘              └─────────────────┘
```

Key changes from current spec:
- **Writer task** decouples main loop from DB latency
- **WriteOp enum** is the typed API (Open, Close, Flip, StopClose, BarScore, MarketSnapshot, CandleClose)
- **Each WriteOp variant maps to one SQL transaction** for stateful transitions
- **State transition detection in executor**, not just in aggregator — executor returns `TradeEvent` enum that main.rs forwards to writer task

## Test Plan (delta from current spec)

**Required test additions** (in order of importance):

1. **Cooldown write amplification test** — simulate 100 bars with no position, assert cooldown is NOT rewritten each bar
2. **Transaction atomicity test** — kill process mid-transaction (mock), assert no impossible state on restart
3. **Cross-process recovery** — process 1 opens position + closes → process 2 starts, asserts correct state including cooldown
4. **DB unreachable at startup** — pool init fails, assert fail-open mode (no trading until recovery) vs current spec's fatal error
5. **Fallback transition** — inject DB failure mid-write, assert JSONL has exactly one line and pool flipped atomically
6. **Cooldown boundary timing** — bar close exactly at `cooldown_until_ts`, assert deterministic behavior
7. **Migration idempotency** — run twice, second is no-op
8. **Candle gap detection** — insert cache with hole, assert incremental backfill detects gap > 1 bar and refetches full range
9. **Config change cooldown** — persist with `cooldown_bars=3`, restart with `cooldown_bars=5`, assert expected behavior (documented rule)
10. **Schema CHECK constraints** — attempt invalid enum values, assert failure

**Test infrastructure requirements**:
- `docker-compose.test.yml` with Postgres 15
- `DATABASE_URL_TEST` env var
- CI GitHub Actions with Postgres service container
- `.sqlx/` offline query cache checked in (for CI without live DB at compile time)

## Required Spec Changes (blocking implementation)

Before writing the implementation plan, the spec needs these updates:

1. **Writer task architecture** — replace "sync await in main loop" with "mpsc-backed writer task" (F1/F14 + Codex #4)
2. **Transactional domain methods** — `record_open(trade + position)`, `record_close(trade + delete_position + cooldown)`, etc. Replace per-table writes. (F15 + Codex #1)
3. **Cooldown state transition fix** — executor returns `TradeEvent::Closed { ... }`; only persist cooldown on real close, not every flat bar (Codex #2 CRITICAL)
4. **Fail-open at startup** — DB connect failure starts in JSONL-only mode with loud warning (F9 + Codex #3, or accept fail-stop and document)
5. **Position/cooldown load = fatal or read-only** — these are trading state (Codex #3)
6. **Missing indexes** — `ts`-only indexes on retention-scanned tables (F16 + Codex #6)
7. **UNIQUE constraints** — `bar_scores(symbol, open_time)`, `trades(symbol, ts, event)` (F22)
8. **CHECK constraints** on enum TEXT fields (F19/F27)
9. **Gap detection in candle_cache** incremental backfill (F6)
10. **Test plan rewrite** — at least 10 scenarios above (F12)
11. **DATABASE_URL redaction** — use `PgConnectOptions` from parts (F18)
12. **Retention batching** — `LIMIT 10000` loop (F21)

## Deferred to TODOS.md (if not already captured)

- CI Postgres service container setup (infra work, sub-project #3 territory)
- Partitioning strategy for `market_data_snapshots` (post-MVP)
- Symbols reference table with CHECK against universe (nice-to-have)

---

# /autoplan Phase 4 — Final Gate

Ready for user decision. See summary below.
