use hyperfun_core::{HlSignalProvider, MarketData};

/// Tracks HLP vault position size.
/// When HLP accumulates shorts, the market is buying = bullish.
/// Score is inverted: HLP short -> positive score (bullish).
pub struct HlpInventorySignal {
    position_size: f64,
    ready: bool,
}

impl HlpInventorySignal {
    pub fn new() -> Self {
        Self {
            position_size: 0.0,
            ready: false,
        }
    }
}

impl Default for HlpInventorySignal {
    fn default() -> Self {
        Self::new()
    }
}

impl HlSignalProvider for HlpInventorySignal {
    fn name(&self) -> &str {
        "hlp_inventory"
    }

    fn update(&mut self, data: &MarketData) {
        if let MarketData::HlpPosition(hlp) = data {
            self.position_size = hlp.position_size;
            self.ready = true;
        }
    }

    fn score(&self) -> f64 {
        // Invert: HLP short (negative position_size) -> positive (bullish)
        let raw = -self.position_size / 10_000_000.0;
        raw.clamp(-1.0, 1.0)
    }

    fn ready(&self) -> bool {
        self.ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyperfun_core::HlpData;

    fn hlp_data(position_size: f64) -> MarketData {
        MarketData::HlpPosition(HlpData {
            symbol: "BTC".to_string(),
            position_size,
            entry_price: 50000.0,
            unrealized_pnl: 0.0,
            timestamp: 1_000_000,
        })
    }

    #[test]
    fn hlp_short_is_bullish() {
        let mut signal = HlpInventorySignal::new();
        // HLP is short $5M (negative position_size)
        signal.update(&hlp_data(-5_000_000.0));
        assert!(signal.ready());
        assert!(signal.score() > 0.0, "HLP short should be bullish (score > 0)");
    }

    #[test]
    fn hlp_long_is_bearish() {
        let mut signal = HlpInventorySignal::new();
        // HLP is long $5M (positive position_size)
        signal.update(&hlp_data(5_000_000.0));
        assert!(signal.ready());
        assert!(signal.score() < 0.0, "HLP long should be bearish (score < 0)");
    }

    #[test]
    fn score_clamped() {
        let mut signal = HlpInventorySignal::new();
        signal.update(&hlp_data(-100_000_000.0));
        assert_eq!(signal.score(), 1.0);

        signal.update(&hlp_data(100_000_000.0));
        assert_eq!(signal.score(), -1.0);
    }

    #[test]
    fn not_ready_before_update() {
        let signal = HlpInventorySignal::new();
        assert!(!signal.ready());
    }

    #[test]
    fn ignores_other_market_data() {
        let mut signal = HlpInventorySignal::new();
        signal.update(&MarketData::Funding(hyperfun_core::FundingData {
            symbol: "BTC".to_string(),
            funding_rate: 0.001,
            predicted_rate: 0.001,
            timestamp: 0,
        }));
        assert!(!signal.ready());
    }
}
