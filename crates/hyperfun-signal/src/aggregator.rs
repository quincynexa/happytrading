use std::collections::HashMap;
use hyperfun_core::{Direction, SignalAction};

pub struct SignalAggregator {
    open_threshold: f64,
    close_threshold: f64,
    cooldown_bars: u64,
    current_positions: HashMap<String, Direction>,
    /// Tracks how many bar-close ticks have elapsed since the last close per symbol.
    /// Absent = no cooldown active.
    cooldown_remaining: HashMap<String, u64>,
}

impl SignalAggregator {
    pub fn new(open_threshold: f64, close_threshold: f64, cooldown_bars: u64) -> Self {
        Self {
            open_threshold,
            close_threshold,
            cooldown_bars,
            current_positions: HashMap::new(),
            cooldown_remaining: HashMap::new(),
        }
    }

    /// Compute a weighted composite score from factor group scores.
    ///
    /// Each entry is (group_name, weight, Option<score>).
    /// Groups with `None` score are excluded entirely. Weights of the
    /// remaining (ready) groups are **renormalized** so they sum to 1.0,
    /// ensuring the composite stays in [-1, 1] regardless of how many
    /// groups are ready.
    /// Returns (composite_score, detail_vec).
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

        let composite = if total_weight > 0.0 {
            weighted_sum / total_weight
        } else {
            0.0
        };

        (composite, details)
    }

    /// Determine what action to take for a symbol given its composite score.
    /// Must be called once per bar-close per symbol so cooldown ticks down correctly.
    pub fn decide(&mut self, symbol: &str, composite_score: f64) -> SignalAction {
        // Tick down cooldown
        let in_cooldown = if let Some(remaining) = self.cooldown_remaining.get_mut(symbol) {
            if *remaining > 0 {
                *remaining -= 1;
                true
            } else {
                self.cooldown_remaining.remove(symbol);
                false
            }
        } else {
            false
        };

        let position = self.current_positions.get(symbol).copied();

        match position {
            None => {
                // During cooldown, suppress new opens
                if in_cooldown {
                    return SignalAction::Hold;
                }
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
                    // Flip to short
                    SignalAction::Open(Direction::Short)
                } else if composite_score.abs() < self.close_threshold {
                    SignalAction::Close
                } else {
                    SignalAction::Hold
                }
            }
            Some(Direction::Short) => {
                if composite_score > self.open_threshold && !in_cooldown {
                    // Flip to long
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

    pub fn clear_position(&mut self, symbol: &str) {
        self.current_positions.remove(symbol);
        // Start cooldown when a position is closed
        if self.cooldown_bars > 0 {
            self.cooldown_remaining.insert(symbol.to_string(), self.cooldown_bars);
        }
    }

    pub fn has_position(&self, symbol: &str) -> bool {
        self.current_positions.contains_key(symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyperfun_core::{Direction, SignalAction};

    #[test]
    fn test_compute_score_weights() {
        let agg = SignalAggregator::new(0.6, 0.2, 0);
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
        let mut agg = SignalAggregator::new(0.6, 0.2, 0);
        let action = agg.decide("BTC", 0.7);
        assert_eq!(action, SignalAction::Open(Direction::Long));
    }

    #[test]
    fn test_decide_hold_when_already_positioned() {
        let mut agg = SignalAggregator::new(0.6, 0.2, 0);
        agg.set_position("BTC", Direction::Long);
        let action = agg.decide("BTC", 0.8);
        assert_eq!(action, SignalAction::Hold);
    }

    #[test]
    fn test_decide_close_when_score_weak() {
        let mut agg = SignalAggregator::new(0.6, 0.2, 0);
        agg.set_position("BTC", Direction::Long);
        let action = agg.decide("BTC", 0.1);
        assert_eq!(action, SignalAction::Close);
    }

    #[test]
    fn test_decide_flip_direction() {
        let mut agg = SignalAggregator::new(0.6, 0.2, 0);
        agg.set_position("BTC", Direction::Long);
        let action = agg.decide("BTC", -0.7);
        assert_eq!(action, SignalAction::Open(Direction::Short));
    }

    #[test]
    fn test_cooldown_suppresses_open_after_close() {
        let mut agg = SignalAggregator::new(0.6, 0.2, 2);
        agg.set_position("BTC", Direction::Long);
        // Close the position
        agg.clear_position("BTC");

        // Bar 1: still in cooldown — should Hold even with strong signal
        let action = agg.decide("BTC", 0.9);
        assert_eq!(action, SignalAction::Hold);

        // Bar 2: still in cooldown
        let action = agg.decide("BTC", 0.9);
        assert_eq!(action, SignalAction::Hold);

        // Bar 3: cooldown expired — should Open
        let action = agg.decide("BTC", 0.9);
        assert_eq!(action, SignalAction::Open(Direction::Long));
    }

    #[test]
    fn test_cooldown_zero_means_no_cooldown() {
        let mut agg = SignalAggregator::new(0.6, 0.2, 0);
        agg.set_position("BTC", Direction::Long);
        agg.clear_position("BTC");

        // Immediately can open again
        let action = agg.decide("BTC", 0.9);
        assert_eq!(action, SignalAction::Open(Direction::Long));
    }
}
