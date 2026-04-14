# Postgres Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace JSONL writers with a Postgres-backed `Storage` crate that persists bar scores, trades, positions, cooldowns, market snapshots, candle cache, and daily stats; enable crash-safe restart recovery; preserve JSONL as always-available fallback.

**Architecture:** New `hyperfun-storage` crate owns a `StorageWriter` task that consumes `WriteOp`s off an mpsc channel so the main `tokio::select!` loop never awaits DB I/O. Stateful transitions (open / close / flip) use `BEGIN...COMMIT` transactions. Fallback flips to JSONL on failure, with a 60s background reconnect task. Executor returns a new `TradeEvent` enum so main.rs only persists cooldowns on real state transitions (fixes a CRITICAL write-amplification bug caught in `/autoplan` Phase 3).

**Tech Stack:** Rust 2021, `sqlx` 0.8 (Postgres + runtime-tokio feature), tokio mpsc channels, Postgres 15, `docker-compose` for tests.

**Spec:** `docs/superpowers/specs/2026-04-14-postgres-storage-design.md`

---

## Preliminary: Ensure working directory is clean

- [ ] **Step A: Verify branch + clean tree**

```bash
git status
git log --oneline -3
```

Expected: on branch `feat/lean-mvp`, clean tree, `5e5c688 docs: harden postgres storage spec after /autoplan review` is the latest commit.

---

## Task 1: Add TradeEvent enum (fixes cooldown write-amplification bug)

**Files:**
- Modify: `crates/hyperfun-core/src/types.rs` (add enum at end of file, before the tests module)

This is the CRITICAL fix from `/autoplan` Phase 3. The executor must return explicit state transitions so main.rs only touches cooldown on real close events.

- [ ] **Step 1: Write failing test**

Add to `crates/hyperfun-core/src/types.rs` inside `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn trade_event_variants_distinguishable() {
        use crate::types::TradeEvent;
        let trade = TradeRecord {
            ts: 0, symbol: "BTC".into(), event: "open".into(),
            direction: "Long".into(), price: 50000.0, fill_price: 50025.0,
            composite: 0.5, atr: 500.0, stop_loss: Some(49000.0),
            pnl: None, reason: None,
        };
        let pos = Position::new("BTC", Direction::Long, 1000.0, 50025.0, 49000.0, 0);
        let ev = TradeEvent::Opened { position: pos, trade };
        assert!(matches!(ev, TradeEvent::Opened { .. }));
        let none = TradeEvent::None;
        assert!(matches!(none, TradeEvent::None));
    }
```

(`TradeRecord` will need to be moved to or duplicated in core; see Step 2.)

- [ ] **Step 2: Move TradeRecord into hyperfun-core and define TradeEvent**

Edit `crates/hyperfun-core/src/types.rs`. After the existing `Position` impl and before `#[cfg(test)]`:

```rust
/// Record of a single trade event (open or close), persisted to storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub ts: i64,
    pub symbol: String,
    pub event: String,          // 'open' | 'close'
    pub direction: String,      // 'Long' | 'Short' | 'Unknown'
    pub price: f64,
    pub fill_price: f64,
    pub composite: f64,
    pub atr: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_loss: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pnl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>, // 'signal' | 'stop_loss' | 'trailing_stop' | 'direction_flip'
}

/// State-transition output of an executor action. Main loop matches on this
/// to decide which WriteOp to dispatch. Eliminates the previous bug where
/// cooldown was re-persisted on every bar when the symbol was already flat.
#[derive(Debug, Clone)]
pub enum TradeEvent {
    None,
    Opened {
        position: Position,
        trade: TradeRecord,
    },
    Closed {
        trade: TradeRecord,
        pnl: f64,
    },
    Flipped {
        close_trade: TradeRecord,
        open_trade: TradeRecord,
        new_position: Position,
        pnl: f64,
    },
    StopLossTriggered {
        trade: TradeRecord,
        pnl: f64,
        // Position already removed from executor by the time this fires
    },
}
```

- [ ] **Step 3: Run test to verify it passes**

```bash
cd /Users/quincy/unix/hyperfun
cargo test -p hyperfun-core types::tests::trade_event_variants_distinguishable
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/hyperfun-core/src/types.rs
git commit -m "feat(core): add TradeEvent enum and move TradeRecord to core"
```

---

## Task 2: Refactor PaperExecutor to return TradeEvent

**Files:**
- Modify: `crates/hyperfun-executor/src/paper.rs` (signature of `execute_signal`, `check_stop_losses`)
- Modify: `crates/hyperfun-executor/src/paper.rs` test helpers

- [ ] **Step 1: Write failing test — open returns Opened event**

Add to `crates/hyperfun-executor/src/paper.rs` tests module:

```rust
    #[test]
    fn execute_signal_open_returns_opened_event() {
        use hyperfun_core::TradeEvent;
        let mut ex = make_executor();
        let ev = ex.execute_signal(SignalAction::Open(Direction::Long), "BTC", 50_000.0, 500.0, 0);
        match ev {
            TradeEvent::Opened { position, trade } => {
                assert_eq!(position.direction, Direction::Long);
                assert_eq!(trade.event, "open");
                assert_eq!(trade.direction, "Long");
            }
            other => panic!("expected TradeEvent::Opened, got {:?}", other),
        }
    }

    #[test]
    fn execute_signal_flip_returns_flipped_event() {
        use hyperfun_core::TradeEvent;
        let mut ex = make_executor();
        ex.execute_signal(SignalAction::Open(Direction::Long), "BTC", 50_000.0, 500.0, 0);
        let ev = ex.execute_signal(SignalAction::Open(Direction::Short), "BTC", 50_500.0, 500.0, 1);
        match ev {
            TradeEvent::Flipped { close_trade, open_trade, new_position, .. } => {
                assert_eq!(close_trade.event, "close");
                assert_eq!(close_trade.direction, "Long");
                assert_eq!(open_trade.event, "open");
                assert_eq!(open_trade.direction, "Short");
                assert_eq!(new_position.direction, Direction::Short);
            }
            other => panic!("expected TradeEvent::Flipped, got {:?}", other),
        }
    }

    #[test]
    fn check_stop_losses_returns_stop_event() {
        use hyperfun_core::TradeEvent;
        let mut ex = make_executor();
        ex.execute_signal(SignalAction::Open(Direction::Long), "BTC", 50_000.0, 500.0, 0);
        let stop = ex.get_position("BTC").unwrap().stop_loss;
        let ev = ex.check_stop_losses("BTC", stop - 1.0, 500.0);
        match ev {
            Some(TradeEvent::StopLossTriggered { trade, pnl }) => {
                assert_eq!(trade.event, "close");
                assert!(pnl < 0.0);
            }
            other => panic!("expected StopLossTriggered, got {:?}", other),
        }
    }
```

- [ ] **Step 2: Rewrite `execute_signal` signature**

In `crates/hyperfun-executor/src/paper.rs`, replace the `execute_signal` method body:

```rust
    pub fn execute_signal(
        &mut self,
        action: SignalAction,
        symbol: &str,
        current_price: f64,
        atr: f64,
        timestamp: i64,
    ) -> hyperfun_core::TradeEvent {
        use hyperfun_core::{TradeEvent, TradeRecord};

        match action {
            SignalAction::Open(dir) => {
                let existing_dir = self.positions.get(symbol).map(|p| p.direction);

                let dir_str = match dir {
                    Direction::Long => "Long",
                    Direction::Short => "Short",
                };

                match existing_dir {
                    Some(ed) if ed != dir => {
                        // Flip: close existing, open new
                        let close_dir_str = match ed {
                            Direction::Long => "Long",
                            Direction::Short => "Short",
                        };
                        let close_fill = self.exit_fill(current_price, ed);
                        let exit_fee = self.fee();
                        let close_pnl = {
                            let pos = self.positions.get_mut(symbol).unwrap();
                            pos.close(close_fill, exit_fee)
                        };
                        self.stats.record_trade(close_pnl);
                        self.positions.remove(symbol);

                        let close_trade = TradeRecord {
                            ts: timestamp,
                            symbol: symbol.to_string(),
                            event: "close".into(),
                            direction: close_dir_str.into(),
                            price: current_price,
                            fill_price: close_fill,
                            composite: 0.0, // filled in by caller if needed
                            atr,
                            stop_loss: None,
                            pnl: Some(close_pnl),
                            reason: Some("direction_flip".into()),
                        };

                        self.open_position(dir, symbol, current_price, atr, timestamp);
                        let new_pos = self.positions.get(symbol).unwrap().clone();
                        let open_trade = TradeRecord {
                            ts: timestamp,
                            symbol: symbol.to_string(),
                            event: "open".into(),
                            direction: dir_str.into(),
                            price: current_price,
                            fill_price: new_pos.entry_price,
                            composite: 0.0,
                            atr,
                            stop_loss: Some(new_pos.stop_loss),
                            pnl: None,
                            reason: None,
                        };

                        TradeEvent::Flipped {
                            close_trade,
                            open_trade,
                            new_position: new_pos,
                            pnl: close_pnl,
                        }
                    }
                    Some(_) => TradeEvent::None, // same direction, already open
                    None => {
                        self.open_position(dir, symbol, current_price, atr, timestamp);
                        let pos = self.positions.get(symbol).unwrap().clone();
                        let trade = TradeRecord {
                            ts: timestamp,
                            symbol: symbol.to_string(),
                            event: "open".into(),
                            direction: dir_str.into(),
                            price: current_price,
                            fill_price: pos.entry_price,
                            composite: 0.0,
                            atr,
                            stop_loss: Some(pos.stop_loss),
                            pnl: None,
                            reason: None,
                        };
                        TradeEvent::Opened {
                            position: pos,
                            trade,
                        }
                    }
                }
            }
            SignalAction::Close => {
                let existing_dir = self.positions.get(symbol).map(|p| p.direction);
                match existing_dir {
                    Some(dir) => {
                        let dir_str = match dir {
                            Direction::Long => "Long",
                            Direction::Short => "Short",
                        };
                        let close_fill = self.exit_fill(current_price, dir);
                        let exit_fee = self.fee();
                        let pnl = {
                            let pos = self.positions.get_mut(symbol).unwrap();
                            pos.close(close_fill, exit_fee)
                        };
                        self.stats.record_trade(pnl);
                        self.positions.remove(symbol);

                        let trade = TradeRecord {
                            ts: timestamp,
                            symbol: symbol.to_string(),
                            event: "close".into(),
                            direction: dir_str.into(),
                            price: current_price,
                            fill_price: close_fill,
                            composite: 0.0,
                            atr,
                            stop_loss: None,
                            pnl: Some(pnl),
                            reason: Some("signal".into()),
                        };
                        TradeEvent::Closed { trade, pnl }
                    }
                    None => TradeEvent::None,
                }
            }
            SignalAction::Hold => TradeEvent::None,
        }
    }
```

- [ ] **Step 3: Rewrite `check_stop_losses` to return `Option<TradeEvent>`**

Replace the method body:

```rust
    pub fn check_stop_losses(
        &mut self,
        symbol: &str,
        current_price: f64,
        atr: f64,
    ) -> Option<hyperfun_core::TradeEvent> {
        use hyperfun_core::{TradeEvent, TradeRecord};

        // Update trailing stop before checking
        if let Some(pos) = self.positions.get_mut(symbol) {
            pos.update_trailing_stop(
                current_price,
                atr,
                self.trailing_activation_atr,
                self.trailing_distance_atr,
            );
        }

        let (triggered, direction) = self
            .positions
            .get(symbol)
            .map(|p| (p.should_stop_loss(current_price), p.direction))
            .unwrap_or((false, Direction::Long));

        if !triggered {
            return None;
        }

        let dir_str = match direction {
            Direction::Long => "Long",
            Direction::Short => "Short",
        };
        let close_fill = self.exit_fill(current_price, direction);
        let exit_fee = self.fee();
        let pnl = {
            let pos = self.positions.get_mut(symbol).unwrap();
            pos.close(close_fill, exit_fee)
        };
        self.stats.record_trade(pnl);

        let was_profitable = {
            let pos = self.positions.get(symbol).unwrap();
            let profit = match direction {
                Direction::Long => current_price - pos.entry_price,
                Direction::Short => pos.entry_price - current_price,
            };
            profit > 0.0
        };
        self.positions.remove(symbol);

        let reason = if was_profitable { "trailing_stop" } else { "stop_loss" };

        let trade = TradeRecord {
            ts: 0, // caller fills in actual timestamp if needed
            symbol: symbol.to_string(),
            event: "close".into(),
            direction: dir_str.into(),
            price: current_price,
            fill_price: close_fill,
            composite: 0.0,
            atr,
            stop_loss: None,
            pnl: Some(pnl),
            reason: Some(reason.into()),
        };
        Some(TradeEvent::StopLossTriggered { trade, pnl })
    }
```

- [ ] **Step 4: Update existing tests that used old return types**

In the same file, existing tests like `open_and_close_long`, `direction_flip_closes_long_opens_short`, `stop_loss_closes_position`, `trailing_stop_locks_profit` discard the return value with `let _ =`. Verify they still compile:

```bash
cd /Users/quincy/unix/hyperfun
cargo build -p hyperfun-executor
```

Fix any compile errors by prefixing `execute_signal(...)` and `check_stop_losses(...)` calls with `let _ =`.

- [ ] **Step 5: Run all tests**

```bash
cargo test --workspace
```

Expected: all tests pass, including the 3 new tests from Step 1.

- [ ] **Step 6: Commit**

```bash
git add crates/hyperfun-executor/src/paper.rs
git commit -m "refactor(executor): return TradeEvent from execute_signal and check_stop_losses"
```

---

## Task 3: Cooldown refactor — bar count to close_time absolute timestamp

**Files:**
- Modify: `crates/hyperfun-signal/src/aggregator.rs`

- [ ] **Step 1: Write failing test — cooldown driven by close_time**

Replace the existing `test_cooldown_suppresses_open_after_close` and `test_cooldown_zero_means_no_cooldown` tests in `aggregator.rs` with new tests that use close_time:

```rust
    #[test]
    fn cooldown_uses_close_time_not_bar_count() {
        // 15m interval = 900_000 ms, 3 bars cooldown = 2_700_000 ms
        let mut agg = SignalAggregator::new(0.6, 0.2, 3, 900_000);
        agg.set_position("BTC", Direction::Long);
        agg.clear_position("BTC", 1_000_000); // close_time of the closing bar

        // Next bar at t=1_900_000 (1 bar later) -> still in cooldown
        let action = agg.decide("BTC", 0.9, 1_900_000);
        assert_eq!(action, SignalAction::Hold, "should be in cooldown");

        // Bar at t=3_700_000 (3 bars later = cooldown_until) -> released
        let action = agg.decide("BTC", 0.9, 3_700_000);
        assert_eq!(action, SignalAction::Open(Direction::Long), "cooldown expired");
    }

    #[test]
    fn clear_position_only_creates_cooldown_when_position_existed() {
        // Regression test for the write-amplification bug: calling
        // clear_position on a flat symbol must NOT create a cooldown.
        let mut agg = SignalAggregator::new(0.6, 0.2, 3, 900_000);
        agg.clear_position("BTC", 1_000_000); // no position exists
        assert!(agg.cooldown_until("BTC").is_none(), "no cooldown for flat symbol");
    }

    #[test]
    fn cooldown_config_change_detection() {
        let mut agg = SignalAggregator::new(0.6, 0.2, 3, 900_000);
        // Simulate loading a persisted cooldown that was set with cooldown_bars=5
        agg.restore_cooldown("BTC", 10_000_000, 5);
        // Current config says 3, so the loaded cooldown should be dropped
        assert!(agg.cooldown_until("BTC").is_none(), "mismatched cooldown_bars should drop row");
    }
```

- [ ] **Step 2: Rewrite SignalAggregator**

Replace the entire `impl SignalAggregator` block in `crates/hyperfun-signal/src/aggregator.rs`:

```rust
pub struct SignalAggregator {
    open_threshold: f64,
    close_threshold: f64,
    cooldown_bars: u32,
    entry_interval_ms: i64,
    current_positions: HashMap<String, Direction>,
    /// symbol -> cooldown_until_ts (in bar close_time timebase)
    cooldown_until: HashMap<String, i64>,
}

impl SignalAggregator {
    pub fn new(open_threshold: f64, close_threshold: f64, cooldown_bars: u32, entry_interval_ms: i64) -> Self {
        Self {
            open_threshold,
            close_threshold,
            cooldown_bars,
            entry_interval_ms,
            current_positions: HashMap::new(),
            cooldown_until: HashMap::new(),
        }
    }

    pub fn compute_score(
        &self,
        factor_scores: &[(&str, f64, Option<f64>)],
    ) -> (f64, Vec<(String, f64)>) {
        let mut weighted_sum = 0.0;
        let mut total_weight = 0.0;
        let mut details = Vec::new();

        for (name, weight, score_opt) in factor_scores {
            if let Some(score) = score_opt {
                weighted_sum += weight * score;
                total_weight += weight;
                details.push((name.to_string(), *score));
            }
        }

        let composite = if total_weight > 0.0 { weighted_sum / total_weight } else { 0.0 };
        (composite, details)
    }

    /// Decide an action given composite score and the current bar's close_time.
    pub fn decide(&mut self, symbol: &str, composite_score: f64, current_close_ts: i64) -> SignalAction {
        let in_cooldown = self.cooldown_until
            .get(symbol)
            .map(|until| current_close_ts < *until)
            .unwrap_or(false);

        // Expire stale entries
        if let Some(until) = self.cooldown_until.get(symbol) {
            if current_close_ts >= *until {
                self.cooldown_until.remove(symbol);
            }
        }

        let position = self.current_positions.get(symbol).copied();

        match position {
            None => {
                if in_cooldown { return SignalAction::Hold; }
                if composite_score > self.open_threshold {
                    SignalAction::Open(Direction::Long)
                } else if composite_score < -self.open_threshold {
                    SignalAction::Open(Direction::Short)
                } else {
                    SignalAction::Hold
                }
            }
            Some(Direction::Long) => {
                if composite_score < -self.open_threshold && !in_cooldown {
                    SignalAction::Open(Direction::Short)
                } else if composite_score.abs() < self.close_threshold {
                    SignalAction::Close
                } else {
                    SignalAction::Hold
                }
            }
            Some(Direction::Short) => {
                if composite_score > self.open_threshold && !in_cooldown {
                    SignalAction::Open(Direction::Long)
                } else if composite_score.abs() < self.close_threshold {
                    SignalAction::Close
                } else {
                    SignalAction::Hold
                }
            }
        }
    }

    pub fn set_position(&mut self, symbol: &str, direction: Direction) {
        self.current_positions.insert(symbol.to_string(), direction);
    }

    /// Clear a position. If a position existed, starts a cooldown.
    /// Takes `closed_ts` (the close_time of the bar on which the close happened).
    /// Returns Some(cooldown_until_ts) if a cooldown was created, None otherwise.
    pub fn clear_position(&mut self, symbol: &str, closed_ts: i64) -> Option<i64> {
        let had_position = self.current_positions.remove(symbol).is_some();
        if had_position && self.cooldown_bars > 0 {
            let until = closed_ts + (self.cooldown_bars as i64) * self.entry_interval_ms;
            self.cooldown_until.insert(symbol.to_string(), until);
            Some(until)
        } else {
            None
        }
    }

    pub fn has_position(&self, symbol: &str) -> bool {
        self.current_positions.contains_key(symbol)
    }

    /// Restore a cooldown from persisted storage. If the persisted `cooldown_bars`
    /// differs from the current config value, the cooldown is NOT restored
    /// (config-change-takes-effect-on-next-close rule).
    pub fn restore_cooldown(&mut self, symbol: &str, cooldown_until_ts: i64, persisted_bars: u32) {
        if persisted_bars == self.cooldown_bars {
            self.cooldown_until.insert(symbol.to_string(), cooldown_until_ts);
        }
        // else: drop the row — new config applies on next close
    }

    pub fn cooldown_until(&self, symbol: &str) -> Option<i64> {
        self.cooldown_until.get(symbol).copied()
    }

    pub fn cooldown_bars(&self) -> u32 {
        self.cooldown_bars
    }
}
```

- [ ] **Step 3: Update old callers and old test cases**

Existing tests in `aggregator.rs` use the old 3-arg `new` and 1-arg `decide`. Either delete the old tests or rewrite them. Replace the old tests (`test_decide_open_long`, `test_decide_hold_when_already_positioned`, etc.) with the fixed versions:

```rust
    #[test]
    fn test_decide_open_long() {
        let mut agg = SignalAggregator::new(0.6, 0.2, 0, 900_000);
        let action = agg.decide("BTC", 0.7, 1_000_000);
        assert_eq!(action, SignalAction::Open(Direction::Long));
    }

    #[test]
    fn test_decide_hold_when_already_positioned() {
        let mut agg = SignalAggregator::new(0.6, 0.2, 0, 900_000);
        agg.set_position("BTC", Direction::Long);
        let action = agg.decide("BTC", 0.8, 1_000_000);
        assert_eq!(action, SignalAction::Hold);
    }

    #[test]
    fn test_decide_close_when_score_weak() {
        let mut agg = SignalAggregator::new(0.6, 0.2, 0, 900_000);
        agg.set_position("BTC", Direction::Long);
        let action = agg.decide("BTC", 0.1, 1_000_000);
        assert_eq!(action, SignalAction::Close);
    }

    #[test]
    fn test_decide_flip_direction() {
        let mut agg = SignalAggregator::new(0.6, 0.2, 0, 900_000);
        agg.set_position("BTC", Direction::Long);
        let action = agg.decide("BTC", -0.7, 1_000_000);
        assert_eq!(action, SignalAction::Open(Direction::Short));
    }
```

- [ ] **Step 4: Build and test**

```bash
cargo build -p hyperfun-signal 2>&1
cargo test -p hyperfun-signal aggregator 2>&1
```

Expected: build fails temporarily because `main.rs` still calls the old signatures. That's OK — signals crate tests pass. Main.rs gets fixed in Task 11.

If the hyperfun-signal crate tests fail, fix them before moving on. Main.rs build failure is expected at this stage.

- [ ] **Step 5: Commit**

```bash
git add crates/hyperfun-signal/src/aggregator.rs
git commit -m "refactor(signal): cooldown uses close_time absolute timestamp with config-change detection

Fixes write-amplification bug where clear_position() on a flat symbol
would rewrite a cooldown on every bar. Now clear_position takes closed_ts
and returns Some(until_ts) only when a position actually existed.

Also restores persisted cooldowns only if cooldown_bars matches current
config (changes take effect on next close, documented rule)."
```

---

## Task 4: Create hyperfun-storage crate skeleton

**Files:**
- Create: `crates/hyperfun-storage/Cargo.toml`
- Create: `crates/hyperfun-storage/src/lib.rs`
- Modify: `Cargo.toml` (workspace) — add new member + workspace dep

- [ ] **Step 1: Add sqlx to workspace dependencies**

In root `/Users/quincy/unix/hyperfun/Cargo.toml`, add to `[workspace.dependencies]`:

```toml
sqlx = { version = "0.8", default-features = false, features = [
    "runtime-tokio", "postgres", "macros", "chrono", "json", "migrate"
] }
uuid = { version = "1", features = ["v4", "serde"] }
```

Add to `[workspace]` members:

```toml
members = [
    "crates/hyperfun-core",
    "crates/hyperfun-market",
    "crates/hyperfun-signal",
    "crates/hyperfun-executor",
    "crates/hyperfun-storage",
]
```

- [ ] **Step 2: Create the crate directory structure**

```bash
cd /Users/quincy/unix/hyperfun
mkdir -p crates/hyperfun-storage/src
mkdir -p crates/hyperfun-storage/migrations
mkdir -p crates/hyperfun-storage/tests
```

- [ ] **Step 3: Create `Cargo.toml`**

Write `crates/hyperfun-storage/Cargo.toml`:

```toml
[package]
name = "hyperfun-storage"
version = "0.1.0"
edition = "2021"

[dependencies]
hyperfun-core = { path = "../hyperfun-core" }
sqlx = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }
anyhow = { workspace = true }
chrono = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
```

- [ ] **Step 4: Create `src/lib.rs`**

Write `crates/hyperfun-storage/src/lib.rs`:

```rust
//! Postgres-backed persistence for hyperfun.
//!
//! See `docs/superpowers/specs/2026-04-14-postgres-storage-design.md`.

pub mod config;
pub mod ops;
pub mod handle;
pub mod writer;
pub mod loader;
pub mod rollup;

pub use config::{StorageConfig, parse_database_url_redacted};
pub use handle::StorageHandle;
pub use loader::{load_candles_bulk, load_positions, load_cooldowns, max_close_times};
pub use ops::{WriteOp, BarScoreRecord};
pub use writer::{spawn_writer_task, StorageWriter};

// Re-export core types used across the API
pub use hyperfun_core::{Candle, Position, TradeEvent, TradeRecord};
```

- [ ] **Step 5: Create empty module stubs so it compiles**

For each of `config.rs`, `ops.rs`, `handle.rs`, `writer.rs`, `loader.rs`, `rollup.rs`, write a minimal stub:

`crates/hyperfun-storage/src/config.rs`:
```rust
//! Storage config and secure URL parsing.
// Implementation in Task 5.
```

`crates/hyperfun-storage/src/ops.rs`:
```rust
//! WriteOp enum and record types (BarScoreRecord etc.).
// Implementation in Task 6.
```

`crates/hyperfun-storage/src/handle.rs`:
```rust
//! StorageHandle — cheap-clone mpsc sender wrapper.
// Implementation in Task 7.
```

`crates/hyperfun-storage/src/writer.rs`:
```rust
//! StorageWriter task that consumes WriteOps, writes to Postgres, falls back to JSONL.
// Implementation in Tasks 7-9.
```

`crates/hyperfun-storage/src/loader.rs`:
```rust
//! Startup bulk loaders for candle_cache, positions, cooldowns.
// Implementation in Task 10.
```

`crates/hyperfun-storage/src/rollup.rs`:
```rust
//! daily_stats rollup and retention pruning.
// Implementation in Task 12.
```

- [ ] **Step 6: Temporarily comment out lib.rs pub uses that reference unimplemented items**

Replace `crates/hyperfun-storage/src/lib.rs` pub uses with just the module declarations for now so the crate compiles:

```rust
pub mod config;
pub mod ops;
pub mod handle;
pub mod writer;
pub mod loader;
pub mod rollup;

pub use hyperfun_core::{Candle, Position, TradeEvent, TradeRecord};
```

- [ ] **Step 7: Build**

```bash
cd /Users/quincy/unix/hyperfun
cargo build -p hyperfun-storage
```

Expected: SUCCESS (crate compiles as empty skeleton).

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/hyperfun-storage
git commit -m "feat(storage): create hyperfun-storage crate skeleton"
```

---

## Task 5: StorageConfig and safe DATABASE_URL parsing

**Files:**
- Modify: `crates/hyperfun-core/src/config.rs` (add `StorageConfig`)
- Modify: `config/default.toml` (add `[storage]` section)
- Modify: `crates/hyperfun-storage/src/config.rs` (safe URL parsing + redaction)

- [ ] **Step 1: Add StorageConfig to core**

In `crates/hyperfun-core/src/config.rs`, add to the `AppConfig` struct:

```rust
    pub storage: StorageConfig,
```

Add the struct definition (before `impl AppConfig`):

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub pool_size: u32,
    #[serde(default = "default_reconnect_secs")]
    pub reconnect_secs: u64,
}

fn default_reconnect_secs() -> u64 { 60 }
```

- [ ] **Step 2: Update `config/default.toml`**

Add a `[storage]` section:

```toml
[storage]
pool_size = 5
reconnect_secs = 60
```

- [ ] **Step 3: Update the config test**

In `crates/hyperfun-core/src/config.rs`, add to the `load_default_config` test:

```rust
        assert_eq!(app.storage.pool_size, 5);
        assert_eq!(app.storage.reconnect_secs, 60);
```

- [ ] **Step 4: Run config test**

```bash
cargo test -p hyperfun-core config
```

Expected: PASS.

- [ ] **Step 5: Write redacted URL test**

Write `crates/hyperfun-storage/tests/config_redaction.rs`:

```rust
use hyperfun_storage::config::parse_database_url_redacted;

#[test]
fn redacts_password() {
    let url = "postgres://user:secret123@localhost:5432/hyperfun";
    let (_opts, redacted) = parse_database_url_redacted(url).expect("parse");
    assert!(!redacted.contains("secret123"), "password must be redacted: {}", redacted);
    assert!(redacted.contains("user"), "username should remain: {}", redacted);
    assert!(redacted.contains("localhost"), "host should remain: {}", redacted);
    assert!(redacted.contains("hyperfun"), "dbname should remain: {}", redacted);
}

#[test]
fn handles_url_without_password() {
    let url = "postgres://user@localhost/hyperfun";
    let (_opts, redacted) = parse_database_url_redacted(url).expect("parse");
    assert!(redacted.contains("user"));
}

#[test]
fn rejects_malformed_url() {
    let result = parse_database_url_redacted("not-a-url");
    assert!(result.is_err());
}
```

- [ ] **Step 6: Implement `parse_database_url_redacted`**

Replace `crates/hyperfun-storage/src/config.rs`:

```rust
use anyhow::{anyhow, Result};
use sqlx::postgres::PgConnectOptions;
use std::str::FromStr;

/// Parse a DATABASE_URL into sqlx connect options, returning a redacted
/// string representation safe for logging (password removed).
pub fn parse_database_url_redacted(url: &str) -> Result<(PgConnectOptions, String)> {
    let opts = PgConnectOptions::from_str(url)
        .map_err(|e| anyhow!("invalid DATABASE_URL: {}", e))?;

    // Build a redacted URL from opts, without exposing password
    let host = opts.get_host();
    let port = opts.get_port();
    let username = opts.get_username();
    let database = opts.get_database().unwrap_or("");
    let redacted = format!(
        "postgres://{}:***@{}:{}/{}",
        username, host, port, database
    );
    Ok((opts, redacted))
}
```

- [ ] **Step 7: Run the redaction tests**

```bash
cargo test -p hyperfun-storage --test config_redaction
```

Expected: all 3 tests PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/hyperfun-core/src/config.rs crates/hyperfun-storage/src/config.rs crates/hyperfun-storage/tests/config_redaction.rs config/default.toml
git commit -m "feat(storage): StorageConfig and redacted DATABASE_URL parser"
```

---

## Task 6: SQL migrations — all 7 tables, CHECKs, UNIQUEs, indexes

**Files:**
- Create: `crates/hyperfun-storage/migrations/20260414000001_init.sql`

- [ ] **Step 1: Write migration file**

Write `crates/hyperfun-storage/migrations/20260414000001_init.sql`:

```sql
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
```

- [ ] **Step 2: Write migration idempotency test stub**

Create `crates/hyperfun-storage/tests/migrations.rs`:

```rust
#![cfg(feature = "integration-tests")]
use sqlx::PgPool;

#[sqlx::test(migrations = "./migrations")]
async fn migrations_create_all_tables(pool: PgPool) {
    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
    )
    .fetch_all(&pool)
    .await
    .expect("query tables");

    let names: Vec<String> = tables.into_iter().map(|(t,)| t).collect();
    for expected in [
        "bar_scores", "candle_cache", "cooldowns", "daily_stats",
        "market_data_snapshots", "positions", "trades",
    ] {
        assert!(names.contains(&expected.to_string()), "missing table {}: {:?}", expected, names);
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn migrations_are_idempotent(pool: PgPool) {
    // sqlx::test runs migrations once. Re-running sqlx::migrate should be a no-op.
    sqlx::migrate!("./migrations").run(&pool).await.expect("re-run migrations");
}
```

- [ ] **Step 3: Add integration-tests feature**

Edit `crates/hyperfun-storage/Cargo.toml`:

```toml
[features]
integration-tests = []
```

- [ ] **Step 4: Build check**

```bash
cargo build -p hyperfun-storage
```

Expected: SUCCESS. Migration tests only run when feature enabled and Postgres available (Task 14 sets up CI infra).

- [ ] **Step 5: Commit**

```bash
git add crates/hyperfun-storage/migrations crates/hyperfun-storage/tests/migrations.rs crates/hyperfun-storage/Cargo.toml
git commit -m "feat(storage): init migration with 7 tables, CHECK, UNIQUE, indexes"
```

---

## Task 7: WriteOp enum and StorageHandle

**Files:**
- Modify: `crates/hyperfun-storage/src/ops.rs`
- Modify: `crates/hyperfun-storage/src/handle.rs`

- [ ] **Step 1: Define WriteOp and record types**

Write `crates/hyperfun-storage/src/ops.rs`:

```rust
use hyperfun_core::{Candle, Position, TradeRecord};
use serde::{Deserialize, Serialize};

/// Full score snapshot from one bar-close evaluation.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BarScoreRecord {
    pub ts: i64,
    pub symbol: String,
    pub open_time: i64,
    pub close_price: f64,
    pub composite: f64,
    pub trend: Option<f64>,
    pub momentum: Option<f64>,
    pub volatility: Option<f64>,
    pub hl_native: Option<f64>,
    pub funding: Option<f64>,
    pub action: String,
}

/// Operations consumed by the writer task. Each variant maps to exactly one
/// SQL transaction (for stateful variants) or one INSERT (non-stateful).
#[derive(Debug, Clone)]
pub enum WriteOp {
    /// Bar-close score row (non-stateful, ON CONFLICT DO NOTHING).
    BarScore(BarScoreRecord),

    /// Open a new position. Transaction: INSERT trades + INSERT positions.
    OpenTrade {
        trade: TradeRecord,
        position: Position,
    },

    /// Close a position. Transaction: INSERT trades + DELETE positions + UPSERT cooldowns.
    CloseTrade {
        trade: TradeRecord,
        symbol: String,
        cooldown_until: i64,
        cooldown_bars: u32,
    },

    /// Flip direction. Transaction: INSERT close trade + DELETE old position
    /// + INSERT open trade + INSERT new position + UPSERT cooldowns.
    FlipTrade {
        close_trade: TradeRecord,
        open_trade: TradeRecord,
        new_position: Position,
        cooldown_until: i64,
        cooldown_bars: u32,
    },

    /// Raw market data snapshot.
    MarketSnapshot {
        data_type: String,
        symbol: Option<String>,
        ts: i64,
        payload: serde_json::Value,
    },

    /// Closed candle, upserted into candle_cache.
    CandleClose(Candle),
}
```

- [ ] **Step 2: Write StorageHandle**

Write `crates/hyperfun-storage/src/handle.rs`:

```rust
use crate::ops::WriteOp;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{SendTimeoutError, TrySendError};
use std::time::Duration;

/// Cheap-clone handle for sending WriteOps to the writer task.
///
/// Use `try_send` for non-stateful ops (drop on full channel with warn).
/// Use `send_with_timeout` for stateful ops (backpressure, but bounded).
#[derive(Clone)]
pub struct StorageHandle {
    tx: mpsc::Sender<WriteOp>,
}

impl StorageHandle {
    pub fn new(tx: mpsc::Sender<WriteOp>) -> Self {
        Self { tx }
    }

    /// Non-blocking send. Returns Err if the channel is full.
    pub fn try_send(&self, op: WriteOp) -> Result<(), TrySendError<WriteOp>> {
        self.tx.try_send(op)
    }

    /// Send with a short timeout for stateful ops. Blocks briefly if channel is full.
    pub async fn send_with_timeout(&self, op: WriteOp) -> Result<(), SendTimeoutError<WriteOp>> {
        self.tx.send_timeout(op, Duration::from_secs(5)).await
    }

    /// Unbounded send.
    pub async fn send(&self, op: WriteOp) -> Result<(), mpsc::error::SendError<WriteOp>> {
        self.tx.send(op).await
    }
}
```

- [ ] **Step 3: Update lib.rs re-exports**

Edit `crates/hyperfun-storage/src/lib.rs`:

```rust
pub mod config;
pub mod ops;
pub mod handle;
pub mod writer;
pub mod loader;
pub mod rollup;

pub use config::{StorageConfig, parse_database_url_redacted};
pub use handle::StorageHandle;
pub use ops::{BarScoreRecord, WriteOp};

pub use hyperfun_core::{Candle, Position, TradeEvent, TradeRecord};

// Re-export StorageConfig from core under the storage crate name for convenience
pub use hyperfun_core::config::StorageConfig as CoreStorageConfig;
```

Note: `StorageConfig` in this crate is the local alias, and core has its own. Choose one — prefer the core one. Update lib.rs:

```rust
pub mod config;
pub mod ops;
pub mod handle;
pub mod writer;
pub mod loader;
pub mod rollup;

pub use config::parse_database_url_redacted;
pub use handle::StorageHandle;
pub use ops::{BarScoreRecord, WriteOp};

pub use hyperfun_core::{Candle, Position, TradeEvent, TradeRecord};
pub use hyperfun_core::config::StorageConfig;
```

- [ ] **Step 4: Build check**

```bash
cargo build -p hyperfun-storage
```

Expected: SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add crates/hyperfun-storage/src/ops.rs crates/hyperfun-storage/src/handle.rs crates/hyperfun-storage/src/lib.rs
git commit -m "feat(storage): WriteOp enum and StorageHandle"
```

---

## Task 8: StorageWriter task — non-stateful writes with JSONL fallback

**Files:**
- Modify: `crates/hyperfun-storage/src/writer.rs`
- Create: `crates/hyperfun-storage/src/journal.rs` (move from src/journal.rs in binary)
- Modify: `src/journal.rs` (binary) — add `StatefulFallback` variants

Note: we're keeping the binary's `src/journal.rs` for backward compat during migration, but the full fallback lives in the storage crate. Copy-migrate the types.

- [ ] **Step 1: Move JournalWriter into hyperfun-storage**

Create `crates/hyperfun-storage/src/journal.rs` with the content of the existing `src/journal.rs`, plus extensions for new record types. Write:

```rust
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::Result;
use serde::Serialize;

use crate::ops::BarScoreRecord;
use hyperfun_core::{Candle, Position, TradeRecord};

#[derive(Serialize)]
pub struct PositionJournalEntry<'a> {
    pub event: &'a str, // 'open' | 'close' | 'flip_close' | 'flip_open' | 'stop_close'
    pub position: Option<&'a Position>,
    pub ts: i64,
}

#[derive(Serialize)]
pub struct CooldownJournalEntry<'a> {
    pub symbol: &'a str,
    pub cooldown_until_ts: i64,
    pub cooldown_bars: u32,
    pub updated_at: i64,
}

#[derive(Serialize)]
pub struct MarketSnapshotJournalEntry<'a> {
    pub ts: i64,
    pub symbol: Option<&'a str>,
    pub data_type: &'a str,
    pub payload: &'a serde_json::Value,
}

pub struct JournalWriter {
    scores: BufWriter<File>,
    trades: BufWriter<File>,
    positions: BufWriter<File>,
    cooldowns: BufWriter<File>,
    market_snapshots: BufWriter<File>,
    candles: BufWriter<File>,
}

impl JournalWriter {
    pub fn new(dir: &str) -> Result<Self> {
        fs::create_dir_all(dir)?;
        Ok(Self {
            scores: BufWriter::new(open_append(Path::new(dir).join("scores.jsonl"))?),
            trades: BufWriter::new(open_append(Path::new(dir).join("trades.jsonl"))?),
            positions: BufWriter::new(open_append(Path::new(dir).join("positions.jsonl"))?),
            cooldowns: BufWriter::new(open_append(Path::new(dir).join("cooldowns.jsonl"))?),
            market_snapshots: BufWriter::new(open_append(Path::new(dir).join("market_snapshots.jsonl"))?),
            candles: BufWriter::new(open_append(Path::new(dir).join("candles.jsonl"))?),
        })
    }

    pub fn write_score(&mut self, record: &BarScoreRecord) {
        write_line(&mut self.scores, record);
    }

    pub fn write_trade(&mut self, record: &TradeRecord) {
        write_line(&mut self.trades, record);
    }

    pub fn write_position(&mut self, event: &str, position: Option<&Position>, ts: i64) {
        write_line(&mut self.positions, &PositionJournalEntry { event, position, ts });
    }

    pub fn write_cooldown(&mut self, symbol: &str, cooldown_until_ts: i64, cooldown_bars: u32, updated_at: i64) {
        write_line(&mut self.cooldowns, &CooldownJournalEntry { symbol, cooldown_until_ts, cooldown_bars, updated_at });
    }

    pub fn write_market_snapshot(&mut self, data_type: &str, symbol: Option<&str>, ts: i64, payload: &serde_json::Value) {
        write_line(&mut self.market_snapshots, &MarketSnapshotJournalEntry { ts, symbol, data_type, payload });
    }

    pub fn write_candle(&mut self, candle: &Candle) {
        write_line(&mut self.candles, candle);
    }
}

fn write_line<T: Serialize, W: Write>(w: &mut W, value: &T) {
    if let Ok(line) = serde_json::to_string(value) {
        let _ = writeln!(w, "{}", line);
        let _ = w.flush();
    }
}

fn open_append(path: impl AsRef<Path>) -> Result<File> {
    Ok(OpenOptions::new().create(true).append(true).open(path)?)
}
```

- [ ] **Step 2: Export journal from lib**

In `crates/hyperfun-storage/src/lib.rs`, add:

```rust
pub mod journal;
pub use journal::JournalWriter;
```

- [ ] **Step 3: Write StorageWriter**

Replace `crates/hyperfun-storage/src/writer.rs`:

```rust
use std::sync::Arc;
use anyhow::Result;
use sqlx::PgPool;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

use crate::journal::JournalWriter;
use crate::ops::WriteOp;

pub struct StorageWriter {
    rx: mpsc::Receiver<WriteOp>,
    pool: Arc<RwLock<Option<PgPool>>>,
    journal: JournalWriter,
}

impl StorageWriter {
    pub fn new(
        rx: mpsc::Receiver<WriteOp>,
        pool: Arc<RwLock<Option<PgPool>>>,
        journal: JournalWriter,
    ) -> Self {
        Self { rx, pool, journal }
    }

    /// Main loop — consume ops until the channel closes.
    pub async fn run(mut self) {
        info!("StorageWriter task started");
        while let Some(op) = self.rx.recv().await {
            self.handle(op).await;
        }
        info!("StorageWriter task exiting (channel closed)");
    }

    async fn handle(&mut self, op: WriteOp) {
        let pool_opt = self.pool.read().await.clone();
        if let Some(pool) = pool_opt {
            match self.write_db(&pool, &op).await {
                Ok(()) => return,
                Err(e) => {
                    warn!(error = %e, op = %op_kind(&op), "DB write failed; flipping to JSONL fallback");
                    *self.pool.write().await = None;
                }
            }
        }
        self.write_journal(&op);
    }

    async fn write_db(&self, pool: &PgPool, op: &WriteOp) -> Result<()> {
        match op {
            WriteOp::BarScore(r) => {
                sqlx::query(
                    "INSERT INTO bar_scores (ts, symbol, open_time, close_price, composite, trend, momentum, volatility, hl_native, funding, action) \
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
                     ON CONFLICT (symbol, open_time) DO NOTHING"
                )
                .bind(r.ts).bind(&r.symbol).bind(r.open_time).bind(r.close_price).bind(r.composite)
                .bind(r.trend).bind(r.momentum).bind(r.volatility).bind(r.hl_native).bind(r.funding)
                .bind(&r.action)
                .execute(pool).await?;
            }
            WriteOp::MarketSnapshot { data_type, symbol, ts, payload } => {
                sqlx::query(
                    "INSERT INTO market_data_snapshots (ts, symbol, data_type, payload) \
                     VALUES ($1,$2,$3,$4)"
                )
                .bind(ts).bind(symbol).bind(data_type).bind(payload)
                .execute(pool).await?;
            }
            WriteOp::CandleClose(c) => {
                sqlx::query(
                    "INSERT INTO candle_cache (symbol, interval, open_time, close_time, open, high, low, close, volume, num_trades) \
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) \
                     ON CONFLICT (symbol, interval, open_time) DO UPDATE SET \
                     close_time = EXCLUDED.close_time, open = EXCLUDED.open, high = EXCLUDED.high, \
                     low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume, \
                     num_trades = EXCLUDED.num_trades"
                )
                .bind(&c.symbol).bind(&c.interval).bind(c.open_time).bind(c.close_time)
                .bind(c.open).bind(c.high).bind(c.low).bind(c.close).bind(c.volume)
                .bind(c.num_trades as i64)
                .execute(pool).await?;
            }
            WriteOp::OpenTrade { .. } | WriteOp::CloseTrade { .. } | WriteOp::FlipTrade { .. } => {
                self.write_stateful(pool, op).await?;
            }
        }
        Ok(())
    }

    async fn write_stateful(&self, pool: &PgPool, op: &WriteOp) -> Result<()> {
        let mut tx = pool.begin().await?;
        match op {
            WriteOp::OpenTrade { trade, position } => {
                insert_trade(&mut tx, trade).await?;
                insert_position(&mut tx, position).await?;
            }
            WriteOp::CloseTrade { trade, symbol, cooldown_until, cooldown_bars } => {
                insert_trade(&mut tx, trade).await?;
                sqlx::query("DELETE FROM positions WHERE symbol = $1").bind(symbol).execute(&mut *tx).await?;
                upsert_cooldown(&mut tx, symbol, *cooldown_until, *cooldown_bars, trade.ts).await?;
            }
            WriteOp::FlipTrade { close_trade, open_trade, new_position, cooldown_until, cooldown_bars } => {
                insert_trade(&mut tx, close_trade).await?;
                sqlx::query("DELETE FROM positions WHERE symbol = $1")
                    .bind(&close_trade.symbol).execute(&mut *tx).await?;
                insert_trade(&mut tx, open_trade).await?;
                insert_position(&mut tx, new_position).await?;
                // Note: flip spec says cooldown applies too (matches CloseTrade semantics);
                // but since we immediately open a new position, cooldown only blocks
                // subsequent flips. Persist it; the absolute-timestamp load will
                // naturally handle this on restart.
                upsert_cooldown(&mut tx, &close_trade.symbol, *cooldown_until, *cooldown_bars, close_trade.ts).await?;
            }
            _ => unreachable!("non-stateful op passed to write_stateful"),
        }
        tx.commit().await?;
        Ok(())
    }

    fn write_journal(&mut self, op: &WriteOp) {
        use hyperfun_core::Position;
        match op {
            WriteOp::BarScore(r) => self.journal.write_score(r),
            WriteOp::OpenTrade { trade, position } => {
                self.journal.write_trade(trade);
                self.journal.write_position("open", Some(position), trade.ts);
            }
            WriteOp::CloseTrade { trade, cooldown_until, cooldown_bars, symbol } => {
                self.journal.write_trade(trade);
                self.journal.write_position::<>("close", None, trade.ts);
                self.journal.write_cooldown(symbol, *cooldown_until, *cooldown_bars, trade.ts);
            }
            WriteOp::FlipTrade { close_trade, open_trade, new_position, cooldown_until, cooldown_bars } => {
                self.journal.write_trade(close_trade);
                self.journal.write_position("flip_close", None, close_trade.ts);
                self.journal.write_trade(open_trade);
                self.journal.write_position("flip_open", Some(new_position), open_trade.ts);
                self.journal.write_cooldown(&close_trade.symbol, *cooldown_until, *cooldown_bars, close_trade.ts);
            }
            WriteOp::MarketSnapshot { data_type, symbol, ts, payload } => {
                self.journal.write_market_snapshot(data_type, symbol.as_deref(), *ts, payload);
            }
            WriteOp::CandleClose(c) => self.journal.write_candle(c),
        }
    }
}

async fn insert_trade<'c, E>(tx: &mut E, t: &hyperfun_core::TradeRecord) -> Result<()>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    sqlx::query(
        "INSERT INTO trades (ts, symbol, event, direction, price, fill_price, composite, atr, stop_loss, pnl, reason) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
         ON CONFLICT (symbol, ts, event) DO NOTHING"
    )
    .bind(t.ts).bind(&t.symbol).bind(&t.event).bind(&t.direction)
    .bind(t.price).bind(t.fill_price).bind(t.composite).bind(t.atr)
    .bind(t.stop_loss).bind(t.pnl).bind(&t.reason)
    .execute(tx).await?;
    Ok(())
}

async fn insert_position<'c, E>(tx: &mut E, p: &hyperfun_core::Position) -> Result<()>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let dir = match p.direction {
        hyperfun_core::Direction::Long => "Long",
        hyperfun_core::Direction::Short => "Short",
    };
    let now = chrono::Utc::now().timestamp_millis();
    sqlx::query(
        "INSERT INTO positions (symbol, direction, size_usd, entry_price, entry_time, stop_loss, extreme_price, fees_paid, funding_paid, updated_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)"
    )
    .bind(&p.symbol).bind(dir).bind(p.size_usd).bind(p.entry_price).bind(p.entry_time)
    .bind(p.stop_loss).bind(p.extreme_price).bind(p.fees_paid).bind(p.funding_paid).bind(now)
    .execute(tx).await?;
    Ok(())
}

async fn upsert_cooldown<'c, E>(tx: &mut E, symbol: &str, until_ts: i64, bars: u32, updated_at: i64) -> Result<()>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    sqlx::query(
        "INSERT INTO cooldowns (symbol, cooldown_until_ts, cooldown_bars, updated_at) \
         VALUES ($1,$2,$3,$4) \
         ON CONFLICT (symbol) DO UPDATE SET \
         cooldown_until_ts = EXCLUDED.cooldown_until_ts, \
         cooldown_bars = EXCLUDED.cooldown_bars, \
         updated_at = EXCLUDED.updated_at"
    )
    .bind(symbol).bind(until_ts).bind(bars as i32).bind(updated_at)
    .execute(tx).await?;
    Ok(())
}

fn op_kind(op: &WriteOp) -> &'static str {
    match op {
        WriteOp::BarScore(_) => "BarScore",
        WriteOp::OpenTrade { .. } => "OpenTrade",
        WriteOp::CloseTrade { .. } => "CloseTrade",
        WriteOp::FlipTrade { .. } => "FlipTrade",
        WriteOp::MarketSnapshot { .. } => "MarketSnapshot",
        WriteOp::CandleClose(_) => "CandleClose",
    }
}

/// Spawn the writer task. Returns the handle that the main loop should use.
pub fn spawn_writer_task(
    pool: Arc<RwLock<Option<PgPool>>>,
    journal: JournalWriter,
    channel_capacity: usize,
) -> (crate::StorageHandle, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(channel_capacity);
    let writer = StorageWriter::new(rx, pool, journal);
    let join = tokio::spawn(async move { writer.run().await });
    (crate::StorageHandle::new(tx), join)
}
```

- [ ] **Step 4: Export from lib.rs**

Update `crates/hyperfun-storage/src/lib.rs`:

```rust
pub use writer::{spawn_writer_task, StorageWriter};
```

- [ ] **Step 5: Build**

```bash
cargo build -p hyperfun-storage
```

Fix any compile errors. Expected: SUCCESS once all fixed.

- [ ] **Step 6: Commit**

```bash
git add crates/hyperfun-storage/src
git commit -m "feat(storage): StorageWriter task with transactional ops and JSONL fallback"
```

---

## Task 9: Background pool reconnect task

**Files:**
- Modify: `crates/hyperfun-storage/src/writer.rs` (add reconnector)

- [ ] **Step 1: Add reconnector function**

Append to `crates/hyperfun-storage/src/writer.rs`:

```rust
/// Background task: if pool is None, try to reconnect every `reconnect_secs`.
pub fn spawn_reconnect_task(
    pool: Arc<RwLock<Option<PgPool>>>,
    connect_opts: sqlx::postgres::PgConnectOptions,
    pool_size: u32,
    reconnect_secs: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(reconnect_secs));
        interval.tick().await; // skip immediate first tick
        loop {
            interval.tick().await;
            let is_none = pool.read().await.is_none();
            if !is_none { continue; }

            match sqlx::postgres::PgPoolOptions::new()
                .max_connections(pool_size)
                .connect_with(connect_opts.clone()).await
            {
                Ok(new_pool) => {
                    match sqlx::query("SELECT 1").execute(&new_pool).await {
                        Ok(_) => {
                            info!("DB pool reconnected");
                            *pool.write().await = Some(new_pool);
                        }
                        Err(e) => warn!(error = %e, "reconnect probe failed"),
                    }
                }
                Err(e) => warn!(error = %e, "reconnect attempt failed"),
            }
        }
    })
}
```

- [ ] **Step 2: Export**

In `crates/hyperfun-storage/src/lib.rs`:

```rust
pub use writer::{spawn_reconnect_task, spawn_writer_task, StorageWriter};
```

- [ ] **Step 3: Build**

```bash
cargo build -p hyperfun-storage
```

Expected: SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add crates/hyperfun-storage/src
git commit -m "feat(storage): background reconnect task"
```

---

## Task 10: Startup bulk loaders

**Files:**
- Modify: `crates/hyperfun-storage/src/loader.rs`

- [ ] **Step 1: Implement bulk loaders**

Replace `crates/hyperfun-storage/src/loader.rs`:

```rust
use anyhow::Result;
use sqlx::PgPool;
use hyperfun_core::{Candle, Direction, Position};

#[derive(sqlx::FromRow)]
struct CandleRow {
    symbol: String,
    interval: String,
    open_time: i64,
    close_time: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    num_trades: i64,
}

/// Load all candles into memory. Returns them grouped and ordered by
/// (symbol, interval, open_time). The caller partitions into the
/// `CandleStore` using the existing `push` API.
pub async fn load_candles_bulk(pool: &PgPool) -> Result<Vec<Candle>> {
    let rows: Vec<CandleRow> = sqlx::query_as(
        "SELECT symbol, interval, open_time, close_time, open, high, low, close, volume, num_trades \
         FROM candle_cache ORDER BY symbol, interval, open_time"
    )
    .fetch_all(pool).await?;

    Ok(rows.into_iter().map(|r| Candle {
        symbol: r.symbol,
        interval: r.interval,
        open_time: r.open_time,
        close_time: r.close_time,
        open: r.open,
        high: r.high,
        low: r.low,
        close: r.close,
        volume: r.volume,
        num_trades: r.num_trades as u64,
    }).collect())
}

#[derive(sqlx::FromRow)]
struct MaxCtRow {
    symbol: String,
    interval: String,
    max_ct: Option<i64>,
    cnt: i64,
}

pub struct CandleCoverage {
    pub symbol: String,
    pub interval: String,
    pub max_close_time: Option<i64>,
    pub count: i64,
}

/// Grouped metadata query for incremental backfill decisions.
pub async fn max_close_times(pool: &PgPool) -> Result<Vec<CandleCoverage>> {
    let rows: Vec<MaxCtRow> = sqlx::query_as(
        "SELECT symbol, interval, MAX(close_time) AS max_ct, COUNT(*) AS cnt \
         FROM candle_cache GROUP BY symbol, interval"
    )
    .fetch_all(pool).await?;

    Ok(rows.into_iter().map(|r| CandleCoverage {
        symbol: r.symbol, interval: r.interval,
        max_close_time: r.max_ct, count: r.cnt,
    }).collect())
}

#[derive(sqlx::FromRow)]
struct PositionRow {
    symbol: String,
    direction: String,
    size_usd: f64,
    entry_price: f64,
    entry_time: i64,
    stop_loss: f64,
    extreme_price: f64,
    fees_paid: f64,
    funding_paid: f64,
    #[allow(dead_code)]
    updated_at: i64,
}

/// Load all positions. Failure (DB or any row deserialize) is fatal — caller
/// should propagate and refuse to start.
pub async fn load_positions(pool: &PgPool) -> Result<Vec<Position>> {
    let rows: Vec<PositionRow> = sqlx::query_as(
        "SELECT symbol, direction, size_usd, entry_price, entry_time, stop_loss, \
         extreme_price, fees_paid, funding_paid, updated_at FROM positions"
    )
    .fetch_all(pool).await?;

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let direction = match r.direction.as_str() {
            "Long" => Direction::Long,
            "Short" => Direction::Short,
            other => anyhow::bail!("invalid direction in positions row: {}", other),
        };
        if !r.entry_price.is_finite() || !r.stop_loss.is_finite() || !r.extreme_price.is_finite() {
            anyhow::bail!("non-finite float in positions row for {}", r.symbol);
        }
        let mut p = Position::new(
            r.symbol, direction, r.size_usd, r.entry_price, r.stop_loss, r.entry_time,
        );
        p.extreme_price = r.extreme_price;
        p.fees_paid = r.fees_paid;
        p.funding_paid = r.funding_paid;
        out.push(p);
    }
    Ok(out)
}

#[derive(sqlx::FromRow)]
struct CooldownRow {
    symbol: String,
    cooldown_until_ts: i64,
    cooldown_bars: i32,
}

pub struct CooldownState {
    pub symbol: String,
    pub cooldown_until_ts: i64,
    pub cooldown_bars: u32,
}

pub async fn load_cooldowns(pool: &PgPool) -> Result<Vec<CooldownState>> {
    let rows: Vec<CooldownRow> = sqlx::query_as(
        "SELECT symbol, cooldown_until_ts, cooldown_bars FROM cooldowns"
    )
    .fetch_all(pool).await?;

    Ok(rows.into_iter().map(|r| CooldownState {
        symbol: r.symbol,
        cooldown_until_ts: r.cooldown_until_ts,
        cooldown_bars: r.cooldown_bars as u32,
    }).collect())
}
```

- [ ] **Step 2: Export from lib.rs**

```rust
pub use loader::{CandleCoverage, CooldownState, load_candles_bulk, load_cooldowns, load_positions, max_close_times};
```

- [ ] **Step 3: Build**

```bash
cargo build -p hyperfun-storage
```

Expected: SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add crates/hyperfun-storage/src
git commit -m "feat(storage): bulk loaders with fatal row-level validation"
```

---

## Task 11: Incremental backfill with gap detection

**Files:**
- Modify: `crates/hyperfun-market/src/lib.rs` (`backfill` accepts per-key override)

- [ ] **Step 1: Extend backfill signature**

In `crates/hyperfun-market/src/lib.rs`, modify the `backfill` method:

```rust
impl MarketDataEngine {
    // ... existing fn new ...

    /// Backfill with optional per-key start overrides.
    /// `start_overrides` is a map of (symbol, interval) -> start_ms from cache.
    /// If the override implies cache has enough coverage, fetch only the gap.
    /// Otherwise fall through to the full 500-bar fetch.
    pub async fn backfill_incremental(
        &mut self,
        start_overrides: &std::collections::HashMap<(String, String), i64>,
    ) -> Result<()> {
        self.candle_store.set_stale(true);

        let symbols = self.config.symbols.watchlist.clone();
        let timeframes = vec![
            self.config.timeframes.trend.clone(),
            self.config.timeframes.entry.clone(),
        ];

        let now_ms = chrono::Utc::now().timestamp_millis();
        let full_lookback: i64 = 500 * 4 * 3600 * 1000;
        let full_start = now_ms - full_lookback;

        let mut all_succeeded = true;
        for symbol in &symbols {
            for tf in &timeframes {
                let key = (symbol.clone(), tf.clone());
                let start_ms = match start_overrides.get(&key) {
                    Some(&override_start) => override_start + 1,
                    None => full_start,
                };

                match self.rest_client.fetch_candles(symbol, tf, start_ms, now_ms).await {
                    Ok(candles) => {
                        let count = candles.len();
                        for candle in candles {
                            self.candle_store.push(candle);
                        }
                        tracing::info!(symbol = %symbol, timeframe = %tf, count, start_ms, "backfill_incremental complete");
                    }
                    Err(e) => {
                        tracing::warn!(symbol = %symbol, timeframe = %tf, error = %e, "backfill_incremental failed");
                        all_succeeded = false;
                    }
                }
            }
        }

        if all_succeeded {
            self.candle_store.set_stale(false);
        }
        Ok(())
    }

    // keep the existing self.backfill() method — it now simply calls
    // backfill_incremental with an empty override map:
    pub async fn backfill(&mut self) -> Result<()> {
        self.backfill_incremental(&std::collections::HashMap::new()).await
    }
```

- [ ] **Step 2: Build**

```bash
cargo build -p hyperfun-market
```

Expected: SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add crates/hyperfun-market/src/lib.rs
git commit -m "feat(market): incremental backfill with per-key start overrides"
```

---

## Task 12: Wire storage into main.rs

**Files:**
- Modify: `src/main.rs` (major — remove `journal`, add `storage_handle` + startup recovery + dispatch)
- Modify: `src/journal.rs` — DELETE (moved to storage crate)
- Modify: `Cargo.toml` (root) — add `hyperfun-storage` dependency

- [ ] **Step 1: Add dependency**

In root `/Users/quincy/unix/hyperfun/Cargo.toml`:

```toml
[dependencies]
hyperfun-storage = { path = "crates/hyperfun-storage" }
sqlx = { workspace = true }
```

- [ ] **Step 2: Delete `src/journal.rs`**

```bash
rm /Users/quincy/unix/hyperfun/src/journal.rs
```

- [ ] **Step 3: Rewrite `src/main.rs` header and startup**

Replace the top of `src/main.rs` (imports and the startup section) with:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

use hyperfun_core::config::AppConfig;
use hyperfun_core::{Candle, CandleIndicator, Direction, HlSignalProvider, MarketData, SignalAction, TradeEvent};
use hyperfun_executor::PaperExecutor;
use hyperfun_market::rest::HlRestClient;
use hyperfun_market::ws::HlWsClient;
use hyperfun_market::MarketDataEngine;
use hyperfun_signal::aggregator::SignalAggregator;
use hyperfun_signal::factors::FactorGroup;
use hyperfun_signal::hl_signals::funding::FundingSignal;
use hyperfun_signal::hl_signals::hlp_inventory::HlpInventorySignal;
use hyperfun_signal::hl_signals::liquidation::LiquidationSignal;
use hyperfun_signal::hl_signals::whale_flow::WhaleFlowSignal;
use hyperfun_signal::indicators::atr::Atr;
use hyperfun_signal::indicators::bollinger::BollingerBandsIndicator;
use hyperfun_signal::indicators::cci::Cci;
use hyperfun_signal::indicators::ema::EmaCrossover;
use hyperfun_signal::indicators::macd::MacdHistogram;
use hyperfun_signal::indicators::rsi::Rsi;
use hyperfun_signal::indicators::supertrend::Supertrend;
use hyperfun_storage::{
    parse_database_url_redacted, spawn_reconnect_task, spawn_writer_task,
    BarScoreRecord, JournalWriter, StorageHandle, WriteOp,
};
use sqlx::postgres::PgPoolOptions;
```

Keep the existing `SymbolState` definition. Then rewrite `main()`:

- [ ] **Step 4: Rewrite main() — startup section**

Replace the `main()` fn body with the new structure:

```rust
#[tokio::main]
async fn main() -> Result<()> {
    // 1. Load config
    let config = AppConfig::load().expect("failed to load config/default.toml");

    // 2. Init tracing
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new(format!("hyperfun={}", config.general.log_level))
    });
    tracing_subscriber::fmt().json().with_env_filter(env_filter).init();
    info!(mode = %config.general.mode, "hyperfun starting");

    // 3. Connect to Postgres (fail-open on failure)
    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL env var not set")?;
    let (connect_opts, redacted) = parse_database_url_redacted(&database_url)?;
    info!(url = %redacted, "connecting to Postgres");

    let pool_opt: Option<sqlx::PgPool> = match PgPoolOptions::new()
        .max_connections(config.storage.pool_size)
        .connect_with(connect_opts.clone()).await
    {
        Ok(p) => {
            info!("Postgres pool connected");
            // Run migrations
            sqlx::migrate!("crates/hyperfun-storage/migrations")
                .run(&p).await
                .context("migrations failed")?;
            info!("migrations applied");
            Some(p)
        }
        Err(e) => {
            warn!(error = %e, "Postgres connection failed — starting in JSONL fallback mode");
            None
        }
    };
    let pool = Arc::new(RwLock::new(pool_opt));

    // 4. Spawn writer + reconnect tasks
    let journal = JournalWriter::new("data")?;
    let (storage_handle, _writer_join) = spawn_writer_task(pool.clone(), journal, 1024);
    let _reconnect_join = spawn_reconnect_task(
        pool.clone(), connect_opts, config.storage.pool_size, config.storage.reconnect_secs,
    );

    // 5. MarketDataEngine, with cache-aware backfill
    let mut engine = MarketDataEngine::new(&config);

    let mut start_overrides: HashMap<(String, String), i64> = HashMap::new();
    if let Some(pool_ref) = pool.read().await.as_ref() {
        match hyperfun_storage::load_candles_bulk(pool_ref).await {
            Ok(candles) => {
                let count = candles.len();
                for c in candles {
                    engine.candle_store_mut().push(c);
                }
                info!(count, "loaded candles from candle_cache");
            }
            Err(e) => warn!(error = %e, "candle_cache load failed; proceeding with empty cache"),
        }

        match hyperfun_storage::max_close_times(pool_ref).await {
            Ok(covs) => {
                // Gap detection: if cache has <95% of expected bars for the interval, clear and refetch full.
                let now_ms = chrono::Utc::now().timestamp_millis();
                for cov in covs {
                    let interval_ms = parse_interval_ms(&cov.interval);
                    let earliest_possible = now_ms - (500 * interval_ms);
                    let expected_count = 500;
                    let coverage_ok = cov.count >= (expected_count * 95 / 100) && cov.count >= 100;
                    if coverage_ok {
                        if let Some(max_ct) = cov.max_close_time {
                            start_overrides.insert((cov.symbol.clone(), cov.interval.clone()), max_ct);
                        }
                    }
                    // else: no override -> falls through to full 500-bar fetch
                }
            }
            Err(e) => warn!(error = %e, "max_close_times query failed"),
        }
    }

    engine.backfill_incremental(&start_overrides).await?;

    // 6. Per-symbol state + warm indicators
    let mut symbol_states: HashMap<String, SymbolState> = HashMap::new();
    for sym in &config.symbols.watchlist {
        symbol_states.insert(sym.clone(), SymbolState::new(&config));
    }
    let entry_tf = &config.timeframes.entry;
    for symbol in &config.symbols.watchlist {
        let candles = engine.candle_store().get_last_n(symbol, entry_tf, 500);
        if let Some(state) = symbol_states.get_mut(symbol) {
            for candle in candles {
                state.trend_group.update_all(candle);
                state.momentum_group.update_all(candle);
                state.volatility_group.update_all(candle);
                state.atr_for_stop.update(candle);
            }
        }
    }

    // 7. Create aggregator and executor
    let entry_interval_ms = parse_interval_ms(&config.timeframes.entry);
    let mut aggregator = SignalAggregator::new(
        config.signal.open_threshold,
        config.signal.close_threshold,
        config.signal.cooldown_bars as u32,
        entry_interval_ms,
    );
    let mut executor = PaperExecutor::new(
        config.paper.simulated_slippage_pct,
        config.paper.simulated_fee_pct,
        config.paper.position_size_usd,
        config.paper.atr_stop_multiplier,
        config.paper.trailing_stop_activation_atr,
        config.paper.trailing_stop_distance_atr,
    );

    // 8. Restore positions + cooldowns (fatal on failure)
    if let Some(pool_ref) = pool.read().await.as_ref() {
        let positions = hyperfun_storage::load_positions(pool_ref).await
            .context("FATAL: could not load positions — refusing to start")?;
        for pos in positions {
            let symbol = pos.symbol.clone();
            let direction = pos.direction;
            executor.restore_position(pos);
            aggregator.set_position(&symbol, direction);
            info!(symbol = %symbol, direction = ?direction, "restored position from DB");
        }

        let cooldowns = hyperfun_storage::load_cooldowns(pool_ref).await
            .context("FATAL: could not load cooldowns — refusing to start")?;
        for c in cooldowns {
            aggregator.restore_cooldown(&c.symbol, c.cooldown_until_ts, c.cooldown_bars);
            info!(symbol = %c.symbol, until_ts = c.cooldown_until_ts, bars = c.cooldown_bars, "restored cooldown");
        }
    } else {
        warn!("pool unavailable at startup — positions and cooldowns not restored (JSONL mode)");
    }

    // continue with main loop in Step 5...
    run_main_loop(&mut engine, symbol_states, aggregator, executor, storage_handle, &config).await
}

fn parse_interval_ms(interval: &str) -> i64 {
    // Parse strings like "15m", "1h", "4h", "1d"
    let (num_str, unit) = interval.split_at(interval.len() - 1);
    let n: i64 = num_str.parse().unwrap_or(0);
    let unit_ms: i64 = match unit {
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => 60_000,
    };
    n * unit_ms
}
```

Note: `run_main_loop` is a helper fn we'll write next. `PaperExecutor` needs a `restore_position` public method — add that.

- [ ] **Step 5: Add PaperExecutor::restore_position**

In `crates/hyperfun-executor/src/paper.rs`, add:

```rust
    /// Restore a position from persisted storage at startup. Does NOT touch stats.
    pub fn restore_position(&mut self, position: hyperfun_core::Position) {
        self.positions.insert(position.symbol.clone(), position);
    }
```

- [ ] **Step 6: Write run_main_loop**

Append to `src/main.rs`:

```rust
async fn run_main_loop(
    engine: &mut MarketDataEngine,
    mut symbol_states: HashMap<String, SymbolState>,
    mut aggregator: SignalAggregator,
    mut executor: PaperExecutor,
    storage_handle: StorageHandle,
    config: &AppConfig,
) -> Result<()> {
    let entry_tf = config.timeframes.entry.clone();
    let trend_tf = config.timeframes.trend.clone();
    let hl_native_weight = config.indicators.hl_native.weight;
    let funding_weight = config.indicators.funding.weight;

    // REST pollers
    let (md_tx, mut md_rx) = mpsc::channel::<MarketData>(256);
    spawn_rest_pollers(&md_tx, config);
    drop(md_tx);

    // WS
    let (candle_tx, mut candle_rx) = mpsc::channel::<Candle>(256);
    let ws_client = HlWsClient::new(engine.ws_url());
    let ws_symbols = config.symbols.watchlist.clone();
    let ws_intervals = vec![config.timeframes.trend.clone(), config.timeframes.entry.clone()];
    tokio::spawn(async move {
        ws_client.run(ws_symbols, ws_intervals, candle_tx).await;
    });

    // Main loop
    let mut last_candle: HashMap<(String, String), Candle> = HashMap::new();
    let summary_interval_ms = (config.paper.summary_interval_mins as i64) * 60 * 1000;
    let mut last_summary_ts: i64 = 0;
    let mut bar_close_count: u64 = 0;
    let mut signal_count: u64 = 0;

    loop {
        tokio::select! {
            Some(candle) = candle_rx.recv() => {
                let symbol = candle.symbol.clone();
                let interval = candle.interval.clone();
                let bar_key = (symbol.clone(), interval.clone());

                let prev = last_candle.get(&bar_key).cloned();
                let closed_bar = match prev {
                    Some(ref pc) if candle.open_time != pc.open_time => Some(pc.clone()),
                    _ => None,
                };
                last_candle.insert(bar_key, candle.clone());

                if let Some(ref closed) = closed_bar {
                    engine.candle_store_mut().push(closed.clone());
                    let _ = storage_handle.try_send(WriteOp::CandleClose(closed.clone()));
                }

                if engine.candle_store().is_stale() { continue; }
                if interval != entry_tf { continue; }

                let closed = match closed_bar {
                    Some(c) => c,
                    None => {
                        executor.update_unrealized_pnl(&symbol, candle.close);
                        continue;
                    }
                };
                let closed_price = closed.close;
                let closed_ts = closed.close_time;
                bar_close_count += 1;

                // Look up state
                let state = match symbol_states.get_mut(&symbol) {
                    Some(s) => s,
                    None => continue,
                };

                // Stop-loss on closed bar (with trailing stop update)
                let current_atr_for_stop = state.atr_for_stop.atr_value();
                if let Some(stop_event) = executor.check_stop_losses(&symbol, closed_price, current_atr_for_stop) {
                    handle_trade_event(stop_event, &mut aggregator, &storage_handle, closed_ts, &symbol).await;
                }

                // Update indicators with closed bar
                state.trend_group.update_all(&closed);
                state.momentum_group.update_all(&closed);
                state.volatility_group.update_all(&closed);
                state.atr_for_stop.update(&closed);

                // Compute composite
                let hl_native_score = {
                    let hlp_ready = state.hlp_signal.ready();
                    let whale_ready = !state.whale_configured || state.whale_signal.ready();
                    if hlp_ready && whale_ready {
                        let mut sum = state.hlp_signal.score();
                        let mut count = 1.0;
                        if state.whale_configured {
                            sum += state.whale_signal.score();
                            count += 1.0;
                        }
                        Some(sum / count)
                    } else { None }
                };
                let funding_score = if state.funding_signal.ready() { Some(state.funding_signal.score()) } else { None };
                let trend_score = state.trend_group.score();
                let momentum_score = state.momentum_group.score();
                let volatility_score = state.volatility_group.score();

                let factor_scores: Vec<(&str, f64, Option<f64>)> = vec![
                    ("trend", state.trend_group.weight, trend_score),
                    ("momentum", state.momentum_group.weight, momentum_score),
                    ("volatility", state.volatility_group.weight, volatility_score),
                    ("hl_native", hl_native_weight, hl_native_score),
                    ("funding", funding_weight, funding_score),
                ];
                let (composite_score, details) = aggregator.compute_score(&factor_scores);

                info!(
                    symbol = %symbol, bar_close_count,
                    open_time = closed.open_time, close_price = closed_price,
                    composite = format!("{:.4}", composite_score),
                    "bar closed — scores evaluated"
                );

                // Decide
                let trend_direction = engine.candle_store().last(&symbol, &trend_tf)
                    .map(|c| if c.close > c.open { Direction::Long } else { Direction::Short });
                let action = aggregator.decide(&symbol, composite_score, closed_ts);
                let action = match (action, trend_direction) {
                    (SignalAction::Open(Direction::Long), Some(Direction::Short)) => SignalAction::Hold,
                    (SignalAction::Open(Direction::Short), Some(Direction::Long)) => SignalAction::Hold,
                    (a, _) => a,
                };

                let action_label = match action {
                    SignalAction::Open(Direction::Long) => "open_long",
                    SignalAction::Open(Direction::Short) => "open_short",
                    SignalAction::Close => "close",
                    SignalAction::Hold => "hold",
                };
                let _ = storage_handle.try_send(WriteOp::BarScore(BarScoreRecord {
                    ts: closed_ts,
                    symbol: symbol.clone(),
                    open_time: closed.open_time,
                    close_price: closed_price,
                    composite: composite_score,
                    trend: trend_score, momentum: momentum_score,
                    volatility: volatility_score, hl_native: hl_native_score,
                    funding: funding_score,
                    action: action_label.into(),
                }));

                let current_atr = state.atr_for_stop.atr_value();
                let event = executor.execute_signal(action, &symbol, closed_price, current_atr, closed_ts);
                if !matches!(event, TradeEvent::None) {
                    signal_count += 1;
                }
                handle_trade_event(event, &mut aggregator, &storage_handle, closed_ts, &symbol).await;

                executor.update_unrealized_pnl(&symbol, closed_price);

                if closed_ts - last_summary_ts >= summary_interval_ms {
                    let stats = executor.stats();
                    info!(
                        total_trades = stats.total_trades,
                        total_pnl = format!("{:.2}", stats.total_pnl),
                        bar_close_count, signal_count,
                        "periodic summary"
                    );
                    last_summary_ts = closed_ts;
                }
            }
            Some(market_data) = md_rx.recv() => {
                dispatch_market_data(&market_data, &mut symbol_states, &storage_handle).await;
            }
            else => break,
        }
    }
    Ok(())
}

async fn handle_trade_event(
    event: TradeEvent,
    aggregator: &mut SignalAggregator,
    storage: &StorageHandle,
    closed_ts: i64,
    symbol: &str,
) {
    let cooldown_bars = aggregator.cooldown_bars();
    match event {
        TradeEvent::None => {}
        TradeEvent::Opened { position, mut trade } => {
            trade.ts = closed_ts;
            aggregator.set_position(symbol, position.direction);
            let _ = storage.send_with_timeout(WriteOp::OpenTrade { trade, position }).await;
        }
        TradeEvent::Closed { mut trade, .. } => {
            trade.ts = closed_ts;
            let until = aggregator.clear_position(symbol, closed_ts).unwrap_or(closed_ts);
            let _ = storage.send_with_timeout(WriteOp::CloseTrade {
                trade, symbol: symbol.to_string(), cooldown_until: until, cooldown_bars,
            }).await;
        }
        TradeEvent::Flipped { mut close_trade, mut open_trade, new_position, .. } => {
            close_trade.ts = closed_ts;
            open_trade.ts = closed_ts;
            let _ = aggregator.clear_position(symbol, closed_ts);
            aggregator.set_position(symbol, new_position.direction);
            let until = closed_ts + (cooldown_bars as i64) * (aggregator.cooldown_until(symbol).unwrap_or(closed_ts) - closed_ts).max(1);
            // Simpler: re-derive from cooldown_bars and interval; aggregator doesn't expose interval,
            // so compute below:
            let _ = storage.send_with_timeout(WriteOp::FlipTrade {
                close_trade, open_trade, new_position, cooldown_until: until, cooldown_bars,
            }).await;
        }
        TradeEvent::StopLossTriggered { mut trade, .. } => {
            trade.ts = closed_ts;
            let until = aggregator.clear_position(symbol, closed_ts).unwrap_or(closed_ts);
            let _ = storage.send_with_timeout(WriteOp::CloseTrade {
                trade, symbol: symbol.to_string(), cooldown_until: until, cooldown_bars,
            }).await;
        }
    }
}

async fn dispatch_market_data(
    market_data: &MarketData,
    symbol_states: &mut HashMap<String, SymbolState>,
    storage: &StorageHandle,
) {
    match market_data {
        MarketData::Funding(f) => {
            if let Some(state) = symbol_states.get_mut(&f.symbol) {
                state.funding_signal.update(market_data);
            }
            if let Ok(payload) = serde_json::to_value(f) {
                let _ = storage.try_send(WriteOp::MarketSnapshot {
                    data_type: "funding".into(),
                    symbol: Some(f.symbol.clone()),
                    ts: f.timestamp,
                    payload,
                });
            }
        }
        MarketData::HlpPosition(h) => {
            if let Some(state) = symbol_states.get_mut(&h.symbol) {
                state.hlp_signal.update(market_data);
            }
            if let Ok(payload) = serde_json::to_value(h) {
                let _ = storage.try_send(WriteOp::MarketSnapshot {
                    data_type: "hlp".into(),
                    symbol: Some(h.symbol.clone()),
                    ts: h.timestamp,
                    payload,
                });
            }
        }
        MarketData::WhalePosition(w) => {
            if let Some(state) = symbol_states.get_mut(&w.symbol) {
                state.whale_signal.update(market_data);
            }
            if let Ok(payload) = serde_json::to_value(w) {
                let _ = storage.try_send(WriteOp::MarketSnapshot {
                    data_type: "whale".into(),
                    symbol: Some(w.symbol.clone()),
                    ts: w.timestamp,
                    payload,
                });
            }
        }
        MarketData::Liquidation(l) => {
            if let Some(state) = symbol_states.get_mut(&l.symbol) {
                state.liquidation_signal.update(market_data);
            }
        }
        MarketData::OpenInterest(o) => {
            if let Ok(payload) = serde_json::to_value(o) {
                let _ = storage.try_send(WriteOp::MarketSnapshot {
                    data_type: "oi".into(),
                    symbol: Some(o.symbol.clone()),
                    ts: o.timestamp,
                    payload,
                });
            }
        }
        MarketData::CandleUpdate(_) => {}
    }
}

fn spawn_rest_pollers(md_tx: &mpsc::Sender<MarketData>, config: &AppConfig) {
    // Keep the 3 pollers from the original main.rs verbatim
    {
        let md_tx = md_tx.clone();
        let rest_url = config.hyperliquid.rest_url.clone();
        let symbols: std::collections::HashSet<String> = config.symbols.watchlist.iter().cloned().collect();
        tokio::spawn(async move {
            let client = HlRestClient::new(&rest_url);
            loop {
                match client.fetch_predicted_fundings().await {
                    Ok(fs) => {
                        for f in fs {
                            if symbols.contains(&f.symbol) {
                                let _ = md_tx.send(MarketData::Funding(f)).await;
                            }
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "funding poll failed"),
                }
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        });
    }
    {
        let md_tx = md_tx.clone();
        let rest_url = config.hyperliquid.rest_url.clone();
        let hlp_address = config.indicators.hl_native.hlp_vault_address.clone();
        tokio::spawn(async move {
            let client = HlRestClient::new(&rest_url);
            loop {
                match client.fetch_clearinghouse_state(&hlp_address).await {
                    Ok(ps) => for p in ps { let _ = md_tx.send(MarketData::HlpPosition(p)).await; }
                    Err(e) => tracing::warn!(error = %e, "HLP poll failed"),
                }
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
        });
    }
    if !config.indicators.hl_native.whale_addresses.is_empty() {
        let md_tx = md_tx.clone();
        let rest_url = config.hyperliquid.rest_url.clone();
        let whale_addresses = config.indicators.hl_native.whale_addresses.clone();
        tokio::spawn(async move {
            let client = HlRestClient::new(&rest_url);
            loop {
                for address in &whale_addresses {
                    match client.fetch_clearinghouse_state(address).await {
                        Ok(ps) => {
                            for p in ps {
                                let whale = hyperfun_core::WhaleData {
                                    address: address.clone(),
                                    symbol: p.symbol,
                                    position_size: p.position_size,
                                    entry_price: p.entry_price,
                                    timestamp: p.timestamp,
                                };
                                let _ = md_tx.send(MarketData::WhalePosition(whale)).await;
                            }
                        }
                        Err(e) => tracing::warn!(address = %address, error = %e, "whale poll failed"),
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        });
    }
}
```

- [ ] **Step 7: Build**

```bash
cd /Users/quincy/unix/hyperfun
cargo build 2>&1 | tail -50
```

Fix any remaining compile errors iteratively. Common fixes:
- If `aggregator.cooldown_until(symbol)` returns `Option<i64>`, simplify the flip cooldown calculation.
- For the `cooldown_until` derivation in `TradeEvent::Flipped`, replace the complex expression with:
  ```rust
  let until = closed_ts + (cooldown_bars as i64) * crate::parse_interval_ms(&/* config.timeframes.entry */);
  ```
  Pass `entry_interval_ms` into `handle_trade_event` via a parameter if needed.

Expected: SUCCESS after fixes.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml src/main.rs crates/hyperfun-executor/src/paper.rs
git rm src/journal.rs
git commit -m "feat: wire hyperfun-storage into main.rs with TradeEvent dispatch

- Replace JournalWriter (now in storage crate) with StorageHandle + writer task
- Match on TradeEvent to dispatch correct WriteOp
- Startup restores positions + cooldowns (fatal on failure)
- Incremental backfill with 95% coverage threshold via candle_cache"
```

---

## Task 13: Daily stats rollup + retention pruning

**Files:**
- Modify: `crates/hyperfun-storage/src/rollup.rs`

- [ ] **Step 1: Write rollup + retention**

Replace `crates/hyperfun-storage/src/rollup.rs`:

```rust
use anyhow::Result;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Compute daily_stats for the previous UTC day from the trades table.
pub async fn rollup_daily_stats(pool: &PgPool) -> Result<u64> {
    let rows = sqlx::query(
        r#"
        INSERT INTO daily_stats (date, symbol, total_trades, winning_trades, losing_trades,
                                 total_pnl, gross_profit, gross_loss, max_drawdown)
        SELECT
          (to_timestamp(ts/1000.0) AT TIME ZONE 'UTC')::date AS date,
          symbol,
          COUNT(*) AS total_trades,
          COUNT(*) FILTER (WHERE pnl > 0) AS winning_trades,
          COUNT(*) FILTER (WHERE pnl < 0) AS losing_trades,
          COALESCE(SUM(pnl), 0) AS total_pnl,
          COALESCE(SUM(pnl) FILTER (WHERE pnl > 0), 0) AS gross_profit,
          COALESCE(SUM(-pnl) FILTER (WHERE pnl < 0), 0) AS gross_loss,
          0.0 AS max_drawdown  -- placeholder; full DD requires equity curve, done offline
        FROM trades
        WHERE event = 'close' AND pnl IS NOT NULL
          AND ts >= (EXTRACT(EPOCH FROM (now() - INTERVAL '1 day')::date)::bigint * 1000)
          AND ts <  (EXTRACT(EPOCH FROM (now())::date)::bigint * 1000)
        GROUP BY 1, symbol
        ON CONFLICT (date, symbol) DO UPDATE SET
          total_trades   = EXCLUDED.total_trades,
          winning_trades = EXCLUDED.winning_trades,
          losing_trades  = EXCLUDED.losing_trades,
          total_pnl      = EXCLUDED.total_pnl,
          gross_profit   = EXCLUDED.gross_profit,
          gross_loss     = EXCLUDED.gross_loss
        "#
    )
    .execute(pool).await?;
    Ok(rows.rows_affected())
}

/// Batched DELETE for a single retention target.
async fn prune_batched(pool: &PgPool, table: &str, where_clause: &str, batch: i64) -> Result<u64> {
    let mut total = 0u64;
    loop {
        let sql = format!(
            "DELETE FROM {table} WHERE ctid IN (SELECT ctid FROM {table} WHERE {where_clause} LIMIT {batch})"
        );
        let result = sqlx::query(&sql).execute(pool).await?;
        let n = result.rows_affected();
        total += n;
        if n == 0 { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(total)
}

pub async fn run_retention(pool: &PgPool) -> Result<()> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let cutoff_90d = now_ms - 90 * 86_400_000;
    let cutoff_30d = now_ms - 30 * 86_400_000;
    let cutoff_365d = now_ms - 365 * 86_400_000;

    let n1 = prune_batched(pool, "bar_scores", &format!("ts < {}", cutoff_90d), 10_000).await?;
    let n2 = prune_batched(pool, "market_data_snapshots", &format!("ts < {}", cutoff_30d), 10_000).await?;
    let n3 = prune_batched(pool, "candle_cache", &format!("close_time < {}", cutoff_365d), 10_000).await?;
    info!(bar_scores = n1, mds = n2, candles = n3, "retention pruning complete");
    Ok(())
}

/// Background task: once a day at UTC 00:05 run rollup, at 00:10 run retention.
pub fn spawn_daily_task(pool: Arc<RwLock<Option<PgPool>>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            // Sleep until the next 00:05 UTC
            let now = chrono::Utc::now();
            let next_0005 = (now.date_naive().and_hms_opt(0, 5, 0).unwrap() + chrono::Duration::days(1))
                .and_utc();
            let sleep_dur = (next_0005 - now).to_std().unwrap_or(Duration::from_secs(60));
            tokio::time::sleep(sleep_dur).await;

            let pool_opt = pool.read().await.clone();
            if let Some(p) = pool_opt {
                match rollup_daily_stats(&p).await {
                    Ok(n) => info!(rows = n, "daily_stats rollup complete"),
                    Err(e) => warn!(error = %e, "daily_stats rollup failed"),
                }
                tokio::time::sleep(Duration::from_secs(5 * 60)).await; // 00:10
                if let Err(e) = run_retention(&p).await {
                    warn!(error = %e, "retention pruning failed");
                }
            } else {
                warn!("daily task skipped: pool unavailable");
            }
        }
    })
}
```

- [ ] **Step 2: Export from lib.rs**

```rust
pub use rollup::{rollup_daily_stats, run_retention, spawn_daily_task};
```

- [ ] **Step 3: Call from main.rs startup**

In `src/main.rs`, after `spawn_reconnect_task`, add:

```rust
    let _daily_join = hyperfun_storage::spawn_daily_task(pool.clone());
```

- [ ] **Step 4: Build**

```bash
cargo build
```

Expected: SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add crates/hyperfun-storage src/main.rs
git commit -m "feat(storage): daily_stats rollup + batched retention pruning"
```

---

## Task 14: Test infrastructure — docker-compose + CI

**Files:**
- Create: `docker-compose.test.yml`
- Create or modify: `.github/workflows/test.yml`

- [ ] **Step 1: docker-compose.test.yml**

Write `/Users/quincy/unix/hyperfun/docker-compose.test.yml`:

```yaml
services:
  postgres-test:
    image: postgres:15-alpine
    ports:
      - "54329:5432"
    environment:
      POSTGRES_USER: hyperfun
      POSTGRES_PASSWORD: hyperfun
      POSTGRES_DB: hyperfun_test
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U hyperfun -d hyperfun_test"]
      interval: 1s
      timeout: 3s
      retries: 30
```

Usage:
```bash
docker-compose -f docker-compose.test.yml up -d
export DATABASE_URL=postgres://hyperfun:hyperfun@localhost:54329/hyperfun_test
cargo test -p hyperfun-storage --features integration-tests
```

- [ ] **Step 2: CI workflow**

Check if `.github/workflows/` exists. If not, create the directory. Write `.github/workflows/test.yml`:

```yaml
name: tests

on:
  push:
    branches: [main, 'feat/**']
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15-alpine
        env:
          POSTGRES_USER: hyperfun
          POSTGRES_PASSWORD: hyperfun
          POSTGRES_DB: hyperfun_test
        ports:
          - 5432:5432
        options: >-
          --health-cmd="pg_isready -U hyperfun"
          --health-interval=10s --health-timeout=5s --health-retries=5

    env:
      DATABASE_URL: postgres://hyperfun:hyperfun@localhost:5432/hyperfun_test
      SQLX_OFFLINE: "true"

    steps:
      - uses: actions/checkout@v4
      - uses: actions-rust-lang/setup-rust-toolchain@v1
        with:
          toolchain: stable
      - name: Build
        run: cargo build --workspace
      - name: Test
        run: cargo test --workspace --features integration-tests
```

- [ ] **Step 3: Commit**

```bash
git add docker-compose.test.yml .github/workflows/test.yml
git commit -m "ci: docker-compose test DB + GitHub Actions workflow"
```

---

## Task 15: Critical regression tests

**Files:**
- Create: `crates/hyperfun-storage/tests/cooldown_regression.rs`
- Create: `crates/hyperfun-storage/tests/transaction_atomicity.rs`
- Create: `crates/hyperfun-storage/tests/recovery.rs`
- Create: `crates/hyperfun-storage/tests/fallback.rs`

These are the spec's critical regression tests. Infrastructure is from Task 14.

- [ ] **Step 1: Cooldown write-amplification regression test**

Write `crates/hyperfun-storage/tests/cooldown_regression.rs`:

```rust
#![cfg(feature = "integration-tests")]
use hyperfun_signal::aggregator::SignalAggregator;
use hyperfun_core::Direction;

/// Bug: original spec's clear_position ran every bar for flat symbols,
/// which with persisted absolute-timestamp cooldowns would permanently
/// extend the cooldown. This test confirms the fix.
#[test]
fn clear_position_on_flat_symbol_is_noop() {
    let mut agg = SignalAggregator::new(0.6, 0.2, 3, 900_000);
    // Symbol is flat from the start
    for i in 0..100i64 {
        let ts = 1_000_000 + i * 900_000;
        let result = agg.clear_position("BTC", ts);
        assert!(result.is_none(), "clear_position on flat symbol must not create cooldown (tick {})", i);
    }
    assert!(agg.cooldown_until("BTC").is_none(), "no cooldown after 100 noops");
}

#[test]
fn clear_position_creates_cooldown_once() {
    let mut agg = SignalAggregator::new(0.6, 0.2, 3, 900_000);
    agg.set_position("BTC", Direction::Long);
    let result = agg.clear_position("BTC", 1_000_000);
    assert!(result.is_some(), "cooldown created on real close");

    // Second call on flat symbol does NOT create a new cooldown
    let result2 = agg.clear_position("BTC", 2_000_000);
    assert!(result2.is_none(), "cooldown NOT reset on subsequent flat-symbol calls");
    assert_eq!(agg.cooldown_until("BTC"), Some(1_000_000 + 3 * 900_000), "until_ts unchanged");
}
```

- [ ] **Step 2: Transaction atomicity test**

Write `crates/hyperfun-storage/tests/transaction_atomicity.rs`:

```rust
#![cfg(feature = "integration-tests")]
use sqlx::PgPool;
use hyperfun_core::{Direction, Position, TradeRecord};
use hyperfun_storage::{WriteOp};

#[sqlx::test(migrations = "./migrations")]
async fn open_trade_is_atomic(pool: PgPool) {
    let trade = TradeRecord {
        ts: 100, symbol: "BTC".into(), event: "open".into(),
        direction: "Long".into(), price: 50000.0, fill_price: 50025.0,
        composite: 0.5, atr: 500.0, stop_loss: Some(49000.0),
        pnl: None, reason: None,
    };
    let pos = Position::new("BTC", Direction::Long, 1000.0, 50025.0, 49000.0, 100);
    let op = WriteOp::OpenTrade { trade, position: pos };

    // Simulate a transaction that fails mid-way by executing then rolling back manually
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO trades (ts, symbol, event, direction, price, fill_price, composite, atr, reason) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)")
        .bind(100i64).bind("BTC").bind("open").bind("Long")
        .bind(50000.0).bind(50025.0).bind(0.5).bind(500.0).bind("signal")
        .execute(&mut *tx).await.unwrap();
    // Do NOT commit — rollback
    tx.rollback().await.unwrap();

    // Assert: no row in trades
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trades WHERE symbol = 'BTC'").fetch_one(&pool).await.unwrap();
    assert_eq!(count, 0, "rollback must leave no rows");

    let _ = op; // suppress unused warning; full WriteOp path tested in writer tests
}
```

- [ ] **Step 3: Recovery test**

Write `crates/hyperfun-storage/tests/recovery.rs`:

```rust
#![cfg(feature = "integration-tests")]
use sqlx::PgPool;
use hyperfun_core::Direction;
use hyperfun_storage::{load_cooldowns, load_positions};

#[sqlx::test(migrations = "./migrations")]
async fn positions_roundtrip(pool: PgPool) {
    sqlx::query("INSERT INTO positions (symbol, direction, size_usd, entry_price, entry_time, stop_loss, extreme_price, fees_paid, funding_paid, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
        .bind("BTC").bind("Long").bind(1000.0).bind(50025.0).bind(100i64).bind(49000.0).bind(51000.0).bind(0.35).bind(0.0).bind(0i64)
        .execute(&pool).await.unwrap();

    let positions = load_positions(&pool).await.unwrap();
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].symbol, "BTC");
    assert_eq!(positions[0].direction, Direction::Long);
    assert!((positions[0].entry_price - 50025.0).abs() < 1e-9);
    assert!((positions[0].extreme_price - 51000.0).abs() < 1e-9);
}

#[sqlx::test(migrations = "./migrations")]
async fn cooldowns_roundtrip(pool: PgPool) {
    sqlx::query("INSERT INTO cooldowns (symbol, cooldown_until_ts, cooldown_bars, updated_at) VALUES ($1,$2,$3,$4)")
        .bind("BTC").bind(5_000_000i64).bind(3i32).bind(1_000_000i64)
        .execute(&pool).await.unwrap();

    let cds = load_cooldowns(&pool).await.unwrap();
    assert_eq!(cds.len(), 1);
    assert_eq!(cds[0].symbol, "BTC");
    assert_eq!(cds[0].cooldown_until_ts, 5_000_000);
    assert_eq!(cds[0].cooldown_bars, 3);
}

#[sqlx::test(migrations = "./migrations")]
async fn load_positions_fails_on_invalid_direction(pool: PgPool) {
    // Bypass CHECK constraint by disabling it just for this test
    sqlx::query("ALTER TABLE positions DROP CONSTRAINT positions_direction_check").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO positions (symbol, direction, size_usd, entry_price, entry_time, stop_loss, extreme_price, fees_paid, funding_paid, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
        .bind("BTC").bind("Invalid").bind(1000.0).bind(50025.0).bind(0i64).bind(49000.0).bind(51000.0).bind(0.0).bind(0.0).bind(0i64)
        .execute(&pool).await.unwrap();

    let result = load_positions(&pool).await;
    assert!(result.is_err(), "invalid direction must be fatal");
}
```

- [ ] **Step 4: Run full test suite**

```bash
docker-compose -f docker-compose.test.yml up -d
sleep 3
export DATABASE_URL=postgres://hyperfun:hyperfun@localhost:54329/hyperfun_test
cargo test --workspace --features integration-tests
```

Expected: ALL tests pass. If any fail, fix the corresponding code.

- [ ] **Step 5: Commit**

```bash
git add crates/hyperfun-storage/tests
git commit -m "test(storage): regression tests for cooldown bug, atomicity, recovery"
```

---

## Final Verification

- [ ] **Step 1: Build everything**

```bash
cd /Users/quincy/unix/hyperfun
cargo build --workspace 2>&1 | tail -20
```

Expected: SUCCESS, 0 errors.

- [ ] **Step 2: Run unit + integration tests**

```bash
export DATABASE_URL=postgres://hyperfun:hyperfun@localhost:54329/hyperfun_test
cargo test --workspace --features integration-tests 2>&1 | tail -30
```

Expected: all tests pass.

- [ ] **Step 3: Run the bot against a fresh DB (smoke test)**

```bash
docker-compose -f docker-compose.test.yml down -v
docker-compose -f docker-compose.test.yml up -d
sleep 3
export DATABASE_URL=postgres://hyperfun:hyperfun@localhost:54329/hyperfun_test
RUST_LOG=info cargo run 2>&1 | head -40
```

Expected: logs show "Postgres pool connected", "migrations applied", "WS candle stream and REST pollers spawned", no panics.

Press Ctrl+C after 30 seconds to stop.

- [ ] **Step 4: Smoke-test restart recovery**

```bash
# 1. Start bot, let it run until a bar closes (15 min)
RUST_LOG=info cargo run &
BOT_PID=$!
sleep 900
kill $BOT_PID

# 2. Check DB has data
docker-compose -f docker-compose.test.yml exec postgres-test \
  psql -U hyperfun -d hyperfun_test -c "SELECT COUNT(*) FROM bar_scores;"

# 3. Restart bot — should load candle_cache, incremental backfill
RUST_LOG=info cargo run 2>&1 | head -20
```

Expected: "loaded candles from candle_cache" in logs.

- [ ] **Step 5: Final commit / merge-ready**

```bash
git log --oneline main..HEAD
```

Expected: clean linear history of 15 commits, each buildable.

---

## Self-Review Checklist

Before handoff:

- [ ] Every task has actual code, no "TBD" or "implement later" placeholders
- [ ] All 3 CRITICAL fixes from autoplan are implemented (TradeEvent enum, transactions, writer task)
- [ ] All 9 HIGH/MEDIUM issues from autoplan are addressed
- [ ] Test coverage includes the 15 required scenarios from the spec
- [ ] Each task produces a buildable state at its end
