#![cfg(feature = "integration-tests")]
use hyperfun_core::Direction;
use hyperfun_signal::aggregator::SignalAggregator;

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
        assert!(
            result.is_none(),
            "clear_position on flat symbol must not create cooldown (tick {})",
            i
        );
    }
    assert!(
        agg.cooldown_until("BTC").is_none(),
        "no cooldown after 100 noops"
    );
}

#[test]
fn clear_position_creates_cooldown_once() {
    let mut agg = SignalAggregator::new(0.6, 0.2, 3, 900_000);
    agg.set_position("BTC", Direction::Long);
    let result = agg.clear_position("BTC", 1_000_000);
    assert!(result.is_some(), "cooldown created on real close");

    // Second call on flat symbol does NOT create a new cooldown
    let result2 = agg.clear_position("BTC", 2_000_000);
    assert!(
        result2.is_none(),
        "cooldown NOT reset on subsequent flat-symbol calls"
    );
    assert_eq!(
        agg.cooldown_until("BTC"),
        Some(1_000_000 + 3 * 900_000),
        "until_ts unchanged"
    );
}
