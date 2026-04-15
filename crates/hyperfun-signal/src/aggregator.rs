use std::collections::HashMap;
use hyperfun_core::{Direction, SignalAction};

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

#[cfg(test)]
mod tests {
    use super::*;
    use hyperfun_core::{Direction, SignalAction};

    #[test]
    fn test_compute_score_weights() {
        let agg = SignalAggregator::new(0.6, 0.2, 0, 900_000);
        let factors = [
            ("trend", 2.0, Some(0.8f64)),
            ("momentum", 1.0, None),
            ("volatility", 1.0, Some(0.4f64)),
        ];
        let (composite, details) = agg.compute_score(&factors);
        let expected = (2.0 * 0.8 + 1.0 * 0.4) / (2.0 + 1.0);
        assert!((composite - expected).abs() < 1e-9, "composite={} expected={}", composite, expected);
        assert_eq!(details.len(), 2);
    }

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
}
