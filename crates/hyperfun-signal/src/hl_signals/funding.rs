use hyperfun_core::{HlSignalProvider, MarketData};

/// Funding rate signal.
/// Extreme positive funding = crowded longs = bearish.
/// Extreme negative funding = crowded shorts = bullish.
pub struct FundingSignal {
    current_rate: f64,
    predicted_rate: f64,
    /// Threshold at which funding is considered extreme (default: 0.01 = 1%)
    extreme_threshold: f64,
    ready: bool,
}

impl FundingSignal {
    pub fn new(extreme_threshold: f64) -> Self {
        Self {
            current_rate: 0.0,
            predicted_rate: 0.0,
            extreme_threshold,
            ready: false,
        }
    }
}

impl Default for FundingSignal {
    fn default() -> Self {
        Self::new(0.01)
    }
}

impl HlSignalProvider for FundingSignal {
    fn name(&self) -> &str {
        "funding"
    }

    fn update(&mut self, data: &MarketData) {
        if let MarketData::Funding(funding) = data {
            self.current_rate = funding.funding_rate;
            self.predicted_rate = funding.predicted_rate;
            self.ready = true;
        }
    }

    fn score(&self) -> f64 {
        // High positive predicted_rate -> too many longs -> bearish (negative score)
        let raw = -self.predicted_rate / self.extreme_threshold;
        raw.clamp(-1.0, 1.0)
    }

    fn ready(&self) -> bool {
        self.ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyperfun_core::FundingData;

    fn funding_data(funding_rate: f64, predicted_rate: f64) -> MarketData {
        MarketData::Funding(FundingData {
            symbol: "BTC".to_string(),
            funding_rate,
            predicted_rate,
            timestamp: 1_000_000,
        })
    }

    #[test]
    fn high_positive_funding_is_bearish() {
        let mut signal = FundingSignal::default();
        // Predicted rate of 0.005 (positive, but below extreme threshold of 0.01)
        signal.update(&funding_data(0.004, 0.005));
        assert!(signal.ready());
        assert!(signal.score() < 0.0, "High positive funding should be bearish (score < 0)");
    }

    #[test]
    fn negative_funding_is_bullish() {
        let mut signal = FundingSignal::default();
        // Negative predicted rate = crowded shorts = bullish
        signal.update(&funding_data(-0.003, -0.005));
        assert!(signal.ready());
        assert!(signal.score() > 0.0, "Negative funding should be bullish (score > 0)");
    }

    #[test]
    fn extreme_positive_funding_clamped_to_negative_one() {
        let mut signal = FundingSignal::default();
        // Predicted rate equals extreme_threshold -> score = -1.0
        signal.update(&funding_data(0.01, 0.01));
        assert_eq!(signal.score(), -1.0);
    }

    #[test]
    fn extreme_negative_funding_clamped_to_positive_one() {
        let mut signal = FundingSignal::default();
        // Predicted rate equals -extreme_threshold -> score = 1.0
        signal.update(&funding_data(-0.01, -0.01));
        assert_eq!(signal.score(), 1.0);
    }

    #[test]
    fn zero_funding_is_neutral() {
        let mut signal = FundingSignal::default();
        signal.update(&funding_data(0.0, 0.0));
        assert_eq!(signal.score(), 0.0);
    }

    #[test]
    fn not_ready_before_update() {
        let signal = FundingSignal::default();
        assert!(!signal.ready());
    }

    #[test]
    fn custom_threshold() {
        let mut signal = FundingSignal::new(0.005);
        // predicted_rate = 0.005 with threshold = 0.005 -> score = -1.0
        signal.update(&funding_data(0.005, 0.005));
        assert_eq!(signal.score(), -1.0);
    }

    #[test]
    fn ignores_non_funding_data() {
        let mut signal = FundingSignal::default();
        signal.update(&MarketData::HlpPosition(hyperfun_core::HlpData {
            symbol: "BTC".to_string(),
            position_size: 1_000_000.0,
            entry_price: 50000.0,
            unrealized_pnl: 0.0,
            timestamp: 0,
        }));
        assert!(!signal.ready());
    }
}
