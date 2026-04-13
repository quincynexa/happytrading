---
title: Supertrend indicator panics on malformed exchange candle data (high < low)
date: 2026-04-12
category: runtime-errors
module: supertrend-indicator
problem_type: runtime_error
component: service_object
severity: critical
symptoms:
  - "Bot crashes with panic at supertrend.rs when exchange sends candle with high < low"
  - "ta::DataItem::builder().build() returns Err for invalid OHLCV, .unwrap() propagates as panic"
  - "Single malformed candle from exchange kills the entire bot process"
root_cause: missing_validation
resolution_type: code_fix
related_components:
  - atr-indicator
tags:
  - unwrap
  - panic
  - external-data
  - ohlcv
  - defensive-coding
  - ta-crate
  - rust
  - exchange-api
---

# Supertrend indicator panics on malformed exchange candle data (high < low)

## Problem

The Supertrend indicator called `ta::DataItem::builder().build().unwrap()` with raw candle data from the Hyperliquid WebSocket. The `ta` crate enforces OHLCV constraints (high >= close/open, low <= close/open, all non-negative). A single malformed candle from the exchange crashes the entire bot with no graceful degradation.

## Symptoms

- Process panic at `supertrend.rs` on the `.unwrap()` call.
- No error message, no recovery, no fallback. The bot stops processing all symbols.
- Malformed candles can arrive during market disruptions, exchange data feed errors, or WebSocket reconnection.

## What Didn't Work

- (session history) The ATR indicator had already been fixed with defensive clamping by a different implementer, but the Supertrend was implemented in a separate task (Task 5) without applying the same pattern. Two subagents, inconsistent pattern.
- (session history) The initial code review (superpowers:requesting-code-review) did NOT catch this bug. It was only found when /gstack-review dispatched parallel specialist subagents (security + adversarial). The security specialist flagged it at confidence 9/10; the adversarial Claude subagent independently identified it as finding #2. Multi-pass review with specialized focus was required to surface it.
- Unit tests all used well-formed synthetic candles. No test exercised malformed data from an external source.

## Solution

Apply defensive clamping before building `DataItem`, and handle the `Result` with `match` instead of `.unwrap()`:

```rust
// Defensive clamping (same pattern as ATR)
let high = candle.high.max(candle.open).max(candle.close);
let low = candle.low.min(candle.open).min(candle.close).max(0.0);
let volume = candle.volume.max(0.0);

let data_item = match ta::DataItem::builder()
    .high(high)
    .low(low)
    .close(candle.close)
    .open(candle.open)
    .volume(volume)
    .build()
{
    Ok(di) => di,
    Err(_) => return, // skip update on malformed data
};
```

Also added input validation at the REST parsing boundary (`parse_candle_from_value`):

```rust
// Reject non-finite, negative, or inconsistent candle data at parse time
if !open.is_finite() || !high.is_finite() || !low.is_finite()
    || !close.is_finite() || !volume.is_finite()
    || high < low || close <= 0.0 {
    return None;
}
```

Files: `crates/hyperfun-signal/src/indicators/supertrend.rs`, `crates/hyperfun-market/src/rest.rs`

## Why This Works

Defense in depth: two layers of protection.

1. **Parse boundary** (`rest.rs`): Rejects obviously malformed candles before they enter the system. NaN, Infinity, negative prices, and high < low are all caught.
2. **Indicator boundary** (`supertrend.rs`): Clamps values to satisfy the `ta` crate's constraints even if a slightly-off candle passes the parse layer. Uses `match` instead of `.unwrap()` as a final safety net.

The bot continues running on bad data from a single candle rather than crashing. The skipped update means one candle's worth of indicator lag, which is acceptable.

## Prevention

- **NEVER call `.unwrap()` on data derived from external input.** This applies to exchange APIs, WebSocket messages, REST responses, user input, and any data crossing a trust boundary. Use `.ok()`, `.unwrap_or_default()`, or `match` instead.
- **When wrapping a library with constructor constraints, sanitize inputs at the boundary.** The `ta` crate's `DataItem` has documented constraints. Any wrapper should enforce them before calling the constructor.
- **Establish a shared sanitization utility** (e.g., `fn sanitize_ohlcv(candle: &Candle) -> Option<DataItem>`) so the pattern is applied consistently across all indicators. The bug occurred because two implementers handled it differently.
- **Add malformed-data tests** for every component that processes external input. Include: NaN, Infinity, negative values, high < low, zero volume.
- **Use clippy's `unwrap_used` lint** (`#![warn(clippy::unwrap_used)]`) in modules that handle external data.

## Related Issues

- ATR indicator (`crates/hyperfun-signal/src/indicators/atr.rs`) already had the correct pattern before this fix.
- REST response validation added in `parse_candle_from_value` catches the same class of issues at a different layer.
