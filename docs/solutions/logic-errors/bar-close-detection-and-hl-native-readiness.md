---
module: hyperfun-market, hyperfun (main loop)
tags: [websocket, bar-close, mid-bar, indicators, hl_native, whale, observability, threshold]
problem_type: logic-error
---

# Bar-close detection, HL-native readiness, observability, and threshold tuning

## Problems

### 1. whale_addresses=[] blocks hl_native readiness

`hl_native_score` in the main loop required both `hlp_signal.ready()` AND
`whale_signal.ready()`. When `whale_addresses` config is empty, the whale
poller is never spawned, so `whale_signal.ready()` stays `false` forever.
This caused the hl_native factor group (25% weight) to always return `None`,
silently losing a quarter of the composite score.

### 2. Mid-bar updates treated as bar closes

Hyperliquid WS pushes a candle update on every trade — these are **mid-bar
updates** where the candle is still forming. The code was running indicator
updates and signal evaluation on every WS message, causing:
- Indicators computed on incomplete/changing bars
- Signal evaluation running hundreds of times per bar instead of once
- Wasted CPU and noisy logs

### 3. No observability

No structured logging of the decision cycle — no way to see what scores
each factor group produced, how many bars closed vs mid-bar ticks, or how
many signals were generated.

### 4. Thresholds too high

`open_threshold = 0.6` was nearly unreachable. With factor weights summing
to ~0.90 and groups often returning `None` when not ready, the maximum
achievable composite score was often well below 0.6.

## Solutions

### 1. Conditional whale readiness

Added `whale_configured: bool` to `SymbolState`. hl_native readiness now
only requires `whale_signal.ready()` when `whale_addresses` is non-empty:

```rust
let whale_ready = !state.whale_configured || state.whale_signal.ready();
```

When only HLP is configured, hl_native uses HLP score alone.

### 2. Bar-close detector via open_time change tracking

Track `last_open_time` per `(symbol, interval)`. When a new candle arrives
with a different `open_time`, the previous bar is closed:

```rust
let is_bar_close = match prev_open_time {
    Some(prev) => candle.open_time != prev,
    None => false,
};
```

- **Mid-bar ticks**: Store candle, check stop-losses, update unrealized PnL.
  Skip indicators and signal evaluation.
- **Bar-close ticks**: Run full indicator update + signal evaluation pipeline.

### 3. Structured observability

- Every bar-close logs all factor scores, composite, and price
- Mid-bar ticks logged at `debug` level with counter
- Market data updates (funding, HLP, whale) logged at `debug`
- Periodic summary now includes `bar_close_count`, `mid_bar_count`, `signal_count`
- Signal OPEN/CLOSE events include `signal_count` for sequencing

### 4. Lowered thresholds

- `open_threshold`: 0.6 → 0.35
- `close_threshold`: 0.2 → 0.12

These values are reachable when 2-3 factor groups agree directionally.
