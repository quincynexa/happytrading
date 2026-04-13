---
title: Signal aggregator inflates scores by renormalizing partial weights during warmup
date: 2026-04-12
category: logic-errors
module: signal-aggregator
problem_type: logic_error
component: service_object
severity: high
symptoms:
  - Composite signal scores elevated during warmup when fewer than all factor groups are ready
  - Bot opens positions based on incomplete evidence during first ~12 hours of operation
  - Weighted sum divided by partial weight total instead of full weight total
root_cause: logic_error
resolution_type: code_fix
tags:
  - weighted-scoring
  - signal-aggregation
  - renormalization
  - warmup
  - multi-factor
  - rust
---

# Signal aggregator inflates scores by renormalizing partial weights during warmup

## Problem

The signal aggregator's `compute_score()` divided `weighted_sum` by the sum of only the READY groups' weights. This renormalization inflates the composite score when some factor groups haven't warmed up yet, causing the bot to open positions based on incomplete evidence.

## Symptoms

- During warmup, only trend (0.25) and momentum (0.15) groups are ready. A weighted_sum of 0.28 becomes 0.28 / 0.40 = 0.70, exceeding the 0.6 open threshold.
- The bot opens positions when only 40% of the signal evidence is available.
- Severity increases with more unready groups: fewer ready groups = more inflation.

## What Didn't Work

- The spec explicitly stated "weights are NOT renormalized" (Decision #5 in the design audit trail). The implementation divided by `total_weight` anyway. (session history) The test suite passed because the test for `compute_score_weights` did not include a partial-readiness scenario that would expose the inflation. 59/59 tests passed with the bug present.
- (session history) The Codex adversarial review independently observed the downstream effect from the configuration angle: with `whale_addresses = []`, the HL-native group (25% weight) is permanently unavailable, causing permanent inflation. Same root cause, different manifestation.

## Solution

Changed `compute_score` to return `weighted_sum` directly. Since all group weights are defined to sum to ~1.0 in config, the weighted sum is already on a 0-1 scale.

```rust
// BEFORE (wrong):
let composite = if total_weight > 0.0 {
    weighted_sum / total_weight  // renormalizes!
} else {
    0.0
};

// AFTER (correct):
let composite = if details.is_empty() { 0.0 } else { weighted_sum };
```

File: `crates/hyperfun-signal/src/aggregator.rs`

## Why This Works

With non-renormalized weights, a partially-ready engine produces lower composite scores (0.28 not 0.70), naturally suppressing premature signals until all factor groups contribute. The composite score honestly reflects that only partial evidence is available. The open threshold (0.6) was calibrated assuming all weights contribute, so it naturally gates against partial evidence.

## Prevention

- **Never renormalize weights in multi-factor scoring systems with optional components.** The total weight should always reflect the maximum possible contribution, not the currently active contribution.
- **Add a partial-readiness test** for any weighted scoring system. The test should verify that the composite score with 2/6 groups ready is lower than the open threshold, not inflated to appear like a strong signal.
- **Design review flag:** Any `weighted_sum / total_weight` where `total_weight` can be less than the configured sum is a potential renormalization bug.

## Related Issues

- Design spec Decision #5: `docs/superpowers/specs/2026-04-12-hyperfun-trading-bot-design.md` (line 371)
- The fix also required correcting HL-native group readiness semantics: requiring BOTH HLP and whale signals to be ready before the group scores (was OR-based).
