use std::collections::HashMap;
use hyperfun_core::{HlSignalProvider, MarketData};

/// Whale flow signal.
/// Tracks position changes of whale addresses and computes an EMA-like net change.
/// Net buying by whales = bullish.
pub struct WhaleFlowSignal {
    /// address -> last known position size
    positions: HashMap<String, f64>,
    /// EMA-like accumulator of net position changes
    net_change: f64,
    ready: bool,
}

impl WhaleFlowSignal {
    pub fn new() -> Self {
        Self {
            positions: HashMap::new(),
            net_change: 0.0,
            ready: false,
        }
    }
}

impl Default for WhaleFlowSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl HlSignalProvider for WhaleFlowSignal {
    fn name(&self) -> &str {
        "whale_flow"
    }

    fn update(&mut self, data: &MarketData) {
        if let MarketData::WhalePosition(whale) = data {
            let prev = self.positions.get(&whale.address).copied().unwrap_or(0.0);
            let delta = whale.position_size - prev;
            self.positions.insert(whale.address.clone(), whale.position_size);
            // EMA-like decay: blend old signal with new delta
            self.net_change = self.net_change * 0.9 + delta;
            self.ready = true;
        }
    }

    fn score(&self) -> f64 {
        let raw = self.net_change / 1_000_000.0;
        raw.clamp(-1.0, 1.0)
    }

    fn ready(&self) -> bool {
        self.ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyperfun_core::WhaleData;

    fn whale_data(address: &str, position_size: f64) -> MarketData {
        MarketData::WhalePosition(WhaleData {
            address: address.to_string(),
            symbol: "BTC".to_string(),
            position_size,
            entry_price: 50000.0,
            timestamp: 1_000_000,
        })
    }

    #[test]
    fn whale_long_is_bullish() {
        let mut signal = WhaleFlowSignal::new();
        // Whale opens a $500K long (from 0)
        signal.update(&whale_data("0xwhale1", 500_000.0));
        assert!(signal.ready());
        assert!(signal.score() > 0.0, "Whale opening long should be bullish (score > 0)");
    }

    #[test]
    fn whale_short_is_bearish() {
        let mut signal = WhaleFlowSignal::new();
        // Whale opens a -$500K short
        signal.update(&whale_data("0xwhale1", -500_000.0));
        assert!(signal.ready());
        assert!(signal.score() < 0.0, "Whale opening short should be bearish (score < 0)");
    }

    #[test]
    fn decay_reduces_old_signal() {
        let mut signal = WhaleFlowSignal::new();
        signal.update(&whale_data("0xwhale1", 500_000.0));
        let initial_score = signal.score();
        // Whale closes position (back to 0), net_change decays
        signal.update(&whale_data("0xwhale1", 0.0));
        assert!(
            signal.score() < initial_score,
            "Score should decay after position close"
        );
    }

    #[test]
    fn multiple_whales_tracked_independently() {
        let mut signal = WhaleFlowSignal::new();
        // Two whales, both going long
        signal.update(&whale_data("0xwhale1", 300_000.0));
        signal.update(&whale_data("0xwhale2", 300_000.0));
        assert!(signal.score() > 0.0);
    }

    #[test]
    fn not_ready_before_update() {
        let signal = WhaleFlowSignal::new();
        assert!(!signal.ready());
    }

    #[test]
    fn score_clamped() {
        let mut signal = WhaleFlowSignal::new();
        signal.update(&whale_data("0xwhale1", 100_000_000.0));
        assert_eq!(signal.score(), 1.0);
    }
}
