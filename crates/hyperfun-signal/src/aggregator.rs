use std::collections::HashMap;
use hyperfun_core::{Direction, SignalAction};

pub struct SignalAggregator {
    open_threshold: f64,
    close_threshold: f64,
    current_positions: HashMap<String, Direction>,
}

impl SignalAggregator {
    pub fn new(open_threshold: f64, close_threshold: f64) -> Self {
        Self {
            open_threshold,
            close_threshold,
            current_positions: HashMap::new(),
        }
    }

    /// Compute a weighted composite score from factor group scores.
    ///
    /// Each entry is (group_name, weight, Option<score>).
    /// Groups with None score are excluded from both numerator and denominator.
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
    pub fn decide(&mut self, symbol: &str, composite_score: f64) -> SignalAction {
        let position = self.current_positions.get(symbol).copied();

        match position {
            None => {
                if composite_score > self.open_threshold {
                    SignalAction::Open(Direction::Long)
                } else if composite_score < -self.open_threshold {
                    SignalAction::Open(Direction::Short)
                } else {
                    SignalAction::Hold
                }
            }
            Some(Direction::Long) => {
                if composite_score < -self.open_threshold {
                    // Flip to short
                    SignalAction::Open(Direction::Short)
                } else if composite_score.abs() < self.close_threshold {
                    SignalAction::Close
                } else {
                    // score > threshold (hold) or score in [close, open] range (hold)
                    SignalAction::Hold
                }
            }
            Some(Direction::Short) => {
                if composite_score > self.open_threshold {
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
        let agg = SignalAggregator::new(0.6, 0.2);
        // Group A: weight 2.0, score 0.8
        // Group B: weight 1.0, score None (excluded)
        // Group C: weight 1.0, score 0.4
        let factors = [
            ("trend", 2.0, Some(0.8f64)),
            ("momentum", 1.0, None),
            ("volatility", 1.0, Some(0.4f64)),
        ];
        let (composite, details) = agg.compute_score(&factors);
        // weighted_sum = 2.0*0.8 + 1.0*0.4 = 2.0
        // total_weight = 2.0 + 1.0 = 3.0
        // composite = 2.0 / 3.0 ≈ 0.6667
        let expected = (2.0 * 0.8 + 1.0 * 0.4) / (2.0 + 1.0);
        assert!((composite - expected).abs() < 1e-9, "composite={} expected={}", composite, expected);
        assert_eq!(details.len(), 2, "only 2 groups with scores should be in details");
    }

    #[test]
    fn test_decide_open_long() {
        let mut agg = SignalAggregator::new(0.6, 0.2);
        // No position, score above threshold -> Open(Long)
        let action = agg.decide("BTC", 0.7);
        assert_eq!(action, SignalAction::Open(Direction::Long));
    }

    #[test]
    fn test_decide_hold_when_already_positioned() {
        let mut agg = SignalAggregator::new(0.6, 0.2);
        agg.set_position("BTC", Direction::Long);
        // Already Long, score above threshold -> Hold
        let action = agg.decide("BTC", 0.8);
        assert_eq!(action, SignalAction::Hold);
    }

    #[test]
    fn test_decide_close_when_score_weak() {
        let mut agg = SignalAggregator::new(0.6, 0.2);
        agg.set_position("BTC", Direction::Long);
        // Score 0.1 < close_threshold 0.2 -> Close
        let action = agg.decide("BTC", 0.1);
        assert_eq!(action, SignalAction::Close);
    }

    #[test]
    fn test_decide_flip_direction() {
        let mut agg = SignalAggregator::new(0.6, 0.2);
        agg.set_position("BTC", Direction::Long);
        // Long position, score -0.7 below -threshold -> Open(Short) (flip)
        let action = agg.decide("BTC", -0.7);
        assert_eq!(action, SignalAction::Open(Direction::Short));
    }
}
