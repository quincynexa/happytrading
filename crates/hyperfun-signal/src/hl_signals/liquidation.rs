use std::collections::VecDeque;
use hyperfun_core::{Direction, HlSignalProvider, LiquidationData, MarketData};

/// Liquidation cascade signal.
/// Tracks long vs short liquidation volume over a lookback window.
/// Long liquidations (forced selling) = bullish signal.
pub struct LiquidationSignal {
    window_secs: i64,
    events: VecDeque<LiquidationData>,
    ready: bool,
}

impl LiquidationSignal {
    pub fn new(window_secs: i64) -> Self {
        Self {
            window_secs,
            events: VecDeque::new(),
            ready: false,
        }
    }
}

impl Default for LiquidationSignal {
    fn default() -> Self {
        Self::new(300) // 5-minute default window
    }
}

impl HlSignalProvider for LiquidationSignal {
    fn name(&self) -> &str {
        "liquidation"
    }

    fn update(&mut self, data: &MarketData) {
        if let MarketData::Liquidation(liq) = data {
            let cutoff = liq.timestamp - self.window_secs;
            // Prune stale entries
            while let Some(front) = self.events.front() {
                if front.timestamp < cutoff {
                    self.events.pop_front();
                } else {
                    break;
                }
            }
            self.events.push_back(liq.clone());
            self.ready = true;
        }
    }

    fn score(&self) -> f64 {
        let mut long_vol = 0.0f64;
        let mut short_vol = 0.0f64;
        for event in &self.events {
            match event.direction {
                Direction::Long => long_vol += event.size,
                Direction::Short => short_vol += event.size,
            }
        }
        let total = long_vol + short_vol;
        if total == 0.0 {
            return 0.0;
        }
        // Long liqs = forced selling => market absorbs them = bullish
        let raw = (long_vol - short_vol) / total;
        raw.clamp(-1.0, 1.0)
    }

    fn ready(&self) -> bool {
        self.ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn liq(direction: Direction, size: f64, timestamp: i64) -> MarketData {
        MarketData::Liquidation(LiquidationData {
            symbol: "BTC".to_string(),
            direction,
            size,
            price: 50000.0,
            timestamp,
        })
    }

    #[test]
    fn long_liquidations_are_bullish() {
        let mut signal = LiquidationSignal::new(300);
        // 5 long liquidations -> market absorbed forced selling -> bullish
        for i in 0..5 {
            signal.update(&liq(Direction::Long, 100_000.0, 1_000 + i));
        }
        assert!(signal.ready());
        assert!(signal.score() > 0.0, "Long liquidations should be bullish (score > 0)");
    }

    #[test]
    fn short_liquidations_are_bearish() {
        let mut signal = LiquidationSignal::new(300);
        for i in 0..5 {
            signal.update(&liq(Direction::Short, 100_000.0, 1_000 + i));
        }
        assert!(signal.ready());
        assert!(signal.score() < 0.0, "Short liquidations should be bearish (score < 0)");
    }

    #[test]
    fn stale_events_are_pruned() {
        let mut signal = LiquidationSignal::new(300);
        // Old long liquidation at t=0
        signal.update(&liq(Direction::Long, 1_000_000.0, 0));
        // New short liquidation 400s later (past window)
        signal.update(&liq(Direction::Short, 100.0, 400));
        // The old long liq should be pruned; only recent short remains -> bearish
        assert!(signal.score() < 0.0, "Old events should be pruned");
    }

    #[test]
    fn not_ready_before_update() {
        let signal = LiquidationSignal::new(300);
        assert!(!signal.ready());
    }

    #[test]
    fn score_clamped_to_one() {
        let mut signal = LiquidationSignal::new(300);
        signal.update(&liq(Direction::Long, 1_000_000.0, 1_000));
        // Only longs -> score should be exactly 1.0
        assert_eq!(signal.score(), 1.0);
    }
}
