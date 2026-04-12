use hyperfun_core::{Candle, CandleIndicator};

pub struct FactorGroup {
    pub name: String,
    pub weight: f64,
    pub indicators: Vec<Box<dyn CandleIndicator>>,
}

impl FactorGroup {
    pub fn new(name: impl Into<String>, weight: f64) -> Self {
        Self {
            name: name.into(),
            weight,
            indicators: Vec::new(),
        }
    }

    pub fn add_indicator(&mut self, indicator: Box<dyn CandleIndicator>) {
        self.indicators.push(indicator);
    }

    /// True only when ALL indicators are ready. Empty group returns false.
    pub fn ready(&self) -> bool {
        if self.indicators.is_empty() {
            return false;
        }
        self.indicators.iter().all(|ind| ind.ready())
    }

    /// Average of all indicator scores if ready, None otherwise.
    pub fn score(&self) -> Option<f64> {
        if !self.ready() {
            return None;
        }
        let sum: f64 = self.indicators.iter().map(|ind| ind.score()).sum();
        Some(sum / self.indicators.len() as f64)
    }

    /// Update all indicators with a candle.
    pub fn update_all(&mut self, candle: &Candle) {
        for ind in self.indicators.iter_mut() {
            ind.update(candle);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::ema::EmaCrossover;
    use hyperfun_core::Candle;

    fn make_candle(close: f64) -> Candle {
        Candle {
            symbol: "BTC".to_string(),
            interval: "1m".to_string(),
            open_time: 0,
            close_time: 0,
            open: close,
            high: close,
            low: close,
            close,
            volume: 1000.0,
            num_trades: 100,
        }
    }

    #[test]
    fn test_group_not_ready_until_all_indicators_ready() {
        let mut group = FactorGroup::new("trend", 1.0);
        // EMA(5, 10) needs 10 candles; EMA(3, 5) needs 5 candles
        group.add_indicator(Box::new(EmaCrossover::new(5, 10)));
        group.add_indicator(Box::new(EmaCrossover::new(3, 5)));

        // Feed 9 candles — EMA(5,10) not ready yet
        for i in 0..9 {
            group.update_all(&make_candle(100.0 + i as f64));
            assert!(!group.ready(), "group should not be ready after {} candles", i + 1);
            assert!(group.score().is_none());
        }

        // Feed 10th candle — now both are ready
        group.update_all(&make_candle(109.0));
        assert!(group.ready(), "group should be ready after 10 candles");
        assert!(group.score().is_some(), "score should be Some when ready");
    }
}
